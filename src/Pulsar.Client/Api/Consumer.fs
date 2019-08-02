namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Generic
open System
open Pulsar.Client.Internal
open System.Runtime.CompilerServices
open Pulsar.Client.Common
open pulsar.proto
open Microsoft.Extensions.Logging
open System.Threading

type ConsumerException(message) =
    inherit Exception(message)

type SubscriptionMode =
    | Durable
    | NonDurable

type ConsumerState = {
    WaitingChannel: AsyncReplyChannel<Message>
}

type Consumer private (consumerConfig: ConsumerConfiguration, subscriptionMode: SubscriptionMode, lookup: BinaryLookupService) as this =

    let consumerId = Generators.getNextConsumerId()
    let queue = new Queue<Message>()
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<Message>>
    let subscribeTsc = TaskCompletionSource<Consumer>()
    let partitionIndex = -1
    let prefix = sprintf "consumer(%u, %s)" %consumerId consumerConfig.ConsumerName
    // TODO take from configuration
    let subscribeTimeout = DateTime.Now.Add(TimeSpan.FromSeconds(60.0))
    let connectionHandler =
        ConnectionHandler(prefix,
                          lookup,
                          consumerConfig.Topic.CompleteTopicName,
                          (fun () -> this.Mb.Post(ConsumerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ConsumerMessage.ConnectionFailed ex)))

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
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            Log.Logger.LogInformation("{0} subscribed", prefix)
                            let initialFlowCount = consumerConfig.ReceiverQueueSize |> uint32
                            let firstTimeConnect = subscribeTsc.TrySetResult(this)
                            let isDurable = subscriptionMode = SubscriptionMode.Durable;
                            // if the consumer is not partitioned or is re-connected and is partitioned, we send the flow
                            // command to receive messages.
                            // For readers too (isDurable==false), the partition idx will be set though we have to
                            // send available permits immediately after establishing the reader session
                            if (not (firstTimeConnect && partitionIndex > -1 && isDurable) && consumerConfig.ReceiverQueueSize <> 0) then
                                let flowCommand = Commands.newFlow consumerId initialFlowCount
                                let! success = clientCnx.Send flowCommand
                                if success then
                                    Log.Logger.LogInformation("{0} initial flow sent {1}", prefix, initialFlowCount)
                                else
                                    raise (ConnectionFailedOnSend "FlowCommand")
                        with
                        | ex ->
                            clientCnx.RemoveConsumer consumerId
                            Log.Logger.LogError(ex, "{0} failed to subscribe to topic", prefix)
                            if (connectionHandler.IsRetriableError ex) || not (subscribeTsc.TrySetException ex)  then
                                // Either we had already created the consumer once (subscribeFuture.isDone()) or we are
                                // still within the initial timeout budget and we are dealing with a retriable error
                                connectionHandler.ReconnectLater ex
                            else
                                // unable to create new consumer, fail operation
                                connectionHandler.Failed()
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    return! loop state

                | ConsumerMessage.ConnectionClosed clientCnx ->
                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    let clientCnx = clientCnx :?> ClientCnx
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveConsumer(consumerId)
                    return! loop state

                | ConsumerMessage.ConnectionFailed ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > subscribeTimeout && subscribeTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()
                    return! loop state

                | ConsumerMessage.MessageReceived message ->

                    if state.WaitingChannel = nullChannel then
                        Log.Logger.LogDebug("{0} MessageReceived nullchannel", prefix)
                        queue.Enqueue(message)
                        return! loop state
                    else
                        let queueLength = queue.Count
                        Log.Logger.LogDebug("{0} MessageReceived queueLength={1}", prefix, queueLength)
                        if (queueLength = 0) then
                            state.WaitingChannel.Reply <| message
                        else
                            queue.Enqueue(message)
                            state.WaitingChannel.Reply <| queue.Dequeue()
                        return! loop { state with WaitingChannel = nullChannel }

                | ConsumerMessage.GetMessage ch ->

                    if queue.Count > 0 then
                        ch.Reply <| queue.Dequeue()
                        return! loop state
                    else
                        Log.Logger.LogDebug("{0} GetMessage waiting", prefix)
                        return! loop { state with WaitingChannel = ch }

                | ConsumerMessage.Send (payload, channel) ->

                    match connectionHandler.ConnectionState with
                    | Ready conn ->
                        let! success = conn.Send payload
                        if success then
                            Log.Logger.LogDebug("{0} Send complete", prefix)
                        channel.Reply(success)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                        channel.Reply(false)
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
                                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                                response |> PulsarResponseType.GetEmpty
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
                                    let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                                    response |> PulsarResponseType.GetEmpty
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
            return! mb.PostAndAsyncReply(GetMessage)
        }

    member __.AcknowledgeAsync (msg: Message) =
        task {
            let command = Commands.newAck consumerId msg.MessageId CommandAck.AckType.Individual
            let! success = mb.PostAndAsyncReply(fun channel -> Send (command, channel))
            if not success then
                raise (ConnectionFailedOnSend "AcknowledgeAsync")
        }

    member __.RedeliverUnacknowledgedMessagesAsync () =
        task {
            let command = Commands.newRedeliverUnacknowledgedMessages consumerId None
            let! success = mb.PostAndAsyncReply(fun channel -> Send (command, channel))
            if not success then
                raise (ConnectionFailedOnSend "RedeliverUnacknowledgedMessagesAsync")
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
        task {
            do connectionHandler.GrabCnx()
            return! subscribeTsc.Task
        }

    member private __.Mb with get(): MailboxProcessor<ConsumerMessage> = mb

    static member Init(consumerConfig: ConsumerConfiguration, subscriptionMode: SubscriptionMode, lookup: BinaryLookupService) =
        task {
            let consumer = Consumer(consumerConfig, subscriptionMode, lookup)
            return! consumer.InitInternal()
        }



