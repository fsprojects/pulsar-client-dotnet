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

    let mb = MailboxProcessor<ConsumerMessage>.Start(fun inbox ->

        let rec loop (state: ConsumerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ConsumerMessage.ConnectionOpened ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.RegisterConsumer consumerConfig consumerId this.Mb |> Async.AwaitTask
                    | _ ->
                        Log.Logger.LogWarning("Connection opened but connection is not ready")
                    return! loop state
                | ConsumerMessage.ConnectionClosed ->
                    do! connectionHandler.ConnectionClosed()
                    return! loop state
                | ConsumerMessage.MessageRecieved x ->
                    if state.WaitingChannel = nullChannel
                    then
                        queue.Enqueue(x)
                        return! loop state
                    else
                        state.WaitingChannel.Reply(x)
                        return! loop { state with WaitingChannel = nullChannel }
                | ConsumerMessage.GetMessage ch ->
                    match queue.TryDequeue() with
                    | true, msg ->
                        ch.Reply msg
                        return! loop state
                    | false, _ ->
                        return! loop { state with WaitingChannel = ch }
                | ConsumerMessage.Ack (payload, channel) ->
                    match connectionHandler.ConnectionState with
                    | Ready conn ->
                        do! conn.Send payload
                        channel.Reply()
                    | _ ->
                        //TODO put message on schedule
                    return! loop state
                | ConsumerMessage.SendAndForget payload ->
                    match connectionHandler.ConnectionState with
                    | Ready conn ->
                        conn.SendAndForget payload
                    | _ -> ()
                    return! loop state
                | ConsumerMessage.ReachedEndOfTheTopic ->
                    //TODO notify client app that topic end reached
                    connectionHandler.Terminate()
                | ConsumerMessage.Close channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        //TODO check if we should block mb on closing
                        connectionHandler.Closing()
                        // TODO failPendingReceive
                        do! clientCnx.CloseConsumer consumerId |> Async.AwaitTask
                        connectionHandler.Closed()
                    | _ ->
                        connectionHandler.Closed()
                    channel.Reply()
                    return! loop state
                | ConsumerMessage.Unsubscribe channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        //TODO check if we should block mb on closing
                        connectionHandler.Closing()
                        do! clientCnx.UnsubscribeConsumer consumerId |> Async.AwaitTask
                        connectionHandler.Closed()
                    | _ ->
                        connectionHandler.Closed()
                    channel.Reply()
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
            do! mb.PostAndAsyncReply(fun channel -> Ack (command, channel))
            return! Task.FromResult()
        }

    member __.RedeliverUnacknowledgedMessages () =
        let command = Commands.newRedeliverUnacknowledgedMessages consumerId None
        mb.Post(SendAndForget command)

    member __.CloseAsync() =
        mb.PostAndAsyncReply(ConsumerMessage.Close)

    member __.UnsubscribeAsync() =
        mb.PostAndAsyncReply(ConsumerMessage.Unsubscribe)

    member private __.InitInternal() =
        connectionHandler.Connect()

    member private __.Mb with get(): MailboxProcessor<ConsumerMessage> = mb

    static member Init(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =
        let consumer = Consumer(consumerConfig, lookup)
        consumer.InitInternal()
        consumer



