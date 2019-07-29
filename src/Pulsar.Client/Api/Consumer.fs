namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Concurrent
open System
open Pulsar.Client.Internal
open System.Runtime.CompilerServices
open Pulsar.Client.Common
open pulsar.proto
open Microsoft.Extensions.Logging
open System.Threading

type ConsumerException(message) =
    inherit Exception(message)

type ConsumerState = {
    WaitingChannel: AsyncReplyChannel<Message>
}

type Consumer private (consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) as this =

    let consumerId = Generators.getNextConsumerId()
    let queue = new ConcurrentQueue<Message>()
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<Message>>

    let connectionOpened() =
        this.Mb.Post(ConsumerMessage.ConnectionOpened)

    let connectionHandler = ConnectionHandler(lookup, consumerConfig.Topic.CompleteTopicName, connectionOpened)

    let prefix = sprintf "consumer(%u, %s)" %consumerId consumerConfig.ConsumerName

    let mb = MailboxProcessor<ConsumerMessage>.Start(fun inbox ->

        let rec loop (state: ConsumerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ConsumerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting subscribe to topic {1}", prefix, consumerConfig.Topic)
                        clientCnx.AddConsumer consumerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        let payload =
                            Commands.newSubscribe
                                consumerConfig.Topic.CompleteTopicName consumerConfig.SubscriptionName
                                consumerId requestId consumerConfig.ConsumerName consumerConfig.SubscriptionType
                                consumerConfig.SubscriptionInitialPosition
                        do!
                            fun () -> clientCnx.SendAndWaitForReply requestId payload
                            |> PulsarTypes.GetEmpty
                            |> Async.AwaitTask
                        Log.Logger.LogInformation("{0} subscribed", prefix)
                        let initialFlowCount = consumerConfig.ReceiverQueueSize |> uint32
                        let flowCommand =
                            Commands.newFlow consumerId initialFlowCount
                        do! clientCnx.Send flowCommand
                        Log.Logger.LogInformation("{0} initial flow sent {1}", prefix, initialFlowCount)
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    return! loop state

                | ConsumerMessage.ConnectionClosed ->
                    Log.Logger.LogInformation("{0} ConnectionClosed", prefix)
                    do! connectionHandler.ConnectionClosed()
                    return! loop state
                | ConsumerMessage.MessageRecieved x ->
                    if state.WaitingChannel = nullChannel then
                        Log.Logger.LogInformation("{0} MessageRecieved nullchannel", prefix)
                        queue.Enqueue(x)
                        return! loop state
                    else
                        Log.Logger.LogInformation("{0} MessageRecieved direct reply", prefix)
                        state.WaitingChannel.Reply(x)
                        return! loop { state with WaitingChannel = nullChannel }
                | ConsumerMessage.GetMessage ch ->
                    match queue.TryDequeue() with
                    | true, msg ->
                        ch.Reply msg
                        return! loop state
                    | false, _ ->
                        Log.Logger.LogInformation("{0} GetMessage waiting", prefix)
                        return! loop { state with WaitingChannel = ch }
                | ConsumerMessage.Send (payload, channel) ->
                    match connectionHandler.ConnectionState with
                    | Ready conn ->
                        do! conn.Send payload
                        Log.Logger.LogInformation("{0} Send complete", prefix)
                        channel.Reply()
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                        //TODO put message on schedule
                        ()
                    return! loop state
                | ConsumerMessage.ReachedEndOfTheTopic ->
                    Log.Logger.LogWarning("{0} ReachedEndOfTheTopic", prefix)
                    //TODO notify client app that topic end reached
                    connectionHandler.Terminate()
                | ConsumerMessage.Close channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        // TODO failPendingReceive
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseConsumer consumerId requestId
                        task {
                            try
                                do!
                                    fun () -> clientCnx.SendAndWaitForReply requestId payload
                                    |> PulsarTypes.GetEmpty
                                    |> Async.AwaitTask
                                clientCnx.RemoveConsumer(consumerId)
                                connectionHandler.Closed()
                                Log.Logger.LogInformation("{0} closed", prefix)
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} failed to close", prefix)
                                reraize ex
                        } |> channel.Reply
                    | _ ->
                        Log.Logger.LogInformation("{0} can't close since connection already closed", prefix)
                        connectionHandler.Closed()
                        channel.Reply(Task.FromResult())

                    return! loop state
                | ConsumerMessage.Unsubscribe channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        Log.Logger.LogInformation("{0} starting unsubscribe ", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newUnsubscribeConsumer consumerId requestId
                        let newTask =
                            task {
                                try
                                    do!
                                        fun () -> clientCnx.SendAndWaitForReply requestId payload
                                        |> PulsarTypes.GetEmpty
                                        |> Async.AwaitTask
                                    clientCnx.RemoveConsumer(consumerId)
                                    connectionHandler.Closed()
                                    Log.Logger.LogInformation("{0} unsubscribed", prefix)
                                with
                                | ex ->
                                    connectionHandler.SetReady clientCnx
                                    Log.Logger.LogError(ex, "{0} failed to unsubscribe", prefix)
                                    reraize ex
                            }
                        channel.Reply(newTask)
                    | _ ->
                        Log.Logger.LogInformation("{0} can't unsubscribe since connection already closed", prefix)
                        connectionHandler.Closed()
                        channel.Reply(Task.FromResult<unit>())
                    return! loop state
            }
        loop { WaitingChannel = nullChannel }
    )

    member __.ReceiveAsync() =
        task {
            match queue.TryDequeue() with
            | true, msg ->
                return msg
            | false, _ ->
                 return! mb.PostAndAsyncReply(GetMessage)
        }

    member __.AcknowledgeAsync (msg: Message) =
        task {
            let command = Commands.newAck consumerId msg.MessageId CommandAck.AckType.Individual
            do! mb.PostAndAsyncReply(fun channel -> Send (command, channel))
            return! Task.FromResult()
        }

    member __.RedeliverUnacknowledgedMessagesAsync () =
        task {
            let command = Commands.newRedeliverUnacknowledgedMessages consumerId None
            do! mb.PostAndAsyncReply(fun channel -> Send (command, channel))
            return! Task.FromResult()
        }

    member __.CloseAsync() =
        task {
            let! result = mb.PostAndAsyncReply(ConsumerMessage.Close)
            return! result
        }

    member __.UnsubscribeAsync() =
        task {
            let! result = mb.PostAndAsyncReply(ConsumerMessage.Unsubscribe)
            return! result
        }

    member private __.InitInternal() =
        connectionHandler.Connect()

    member private __.Mb with get(): MailboxProcessor<ConsumerMessage> = mb

    static member Init(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =
        let consumer = Consumer(consumerConfig, lookup)
        consumer.InitInternal()
        consumer



