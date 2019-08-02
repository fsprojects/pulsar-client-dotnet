namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open Microsoft.Extensions.Logging
open System.Collections.Generic

type ProducerException(message) =
    inherit Exception(message)

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) as this =
    let producerId = Generators.getNextProducerId()

    let prefix = sprintf "producer(%u, %s)" %producerId producerConfig.ProducerName
    let producerCreatedTsc = TaskCompletionSource<Producer>()
    // TODO take from configuration
    let createProducerTimeout = DateTime.Now.Add(TimeSpan.FromSeconds(60.0))
    let connectionHandler =
        ConnectionHandler(prefix,
                          lookup,
                          producerConfig.Topic.CompleteTopicName,
                          (fun () -> this.Mb.Post(ProducerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ProducerMessage.ConnectionFailed ex)))

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->

        let pendingMessages = Queue<PendingMessage>()

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with

                | ProducerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                        clientCnx.AddProducer producerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        try
                            let payload = Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName producerId requestId
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            let success = response |> PulsarResponseType.GetProducerSuccess
                            Log.Logger.LogInformation("{0} registered with name {1}", prefix, success.GeneratedProducerName)
                            // process pending messages
                            if pendingMessages.Count > 0 then
                                Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
                                while pendingMessages.Count > 0 do
                                    let pendingMessage = pendingMessages.Dequeue()
                                    this.Mb.Post(SendMessage pendingMessage)
                            else
                                producerCreatedTsc.TrySetResult(this) |> ignore
                        with
                        | ex ->
                            clientCnx.RemoveProducer producerId
                            Log.Logger.LogError(ex, "{0} Failed to create", prefix)
                            // TODO handle special cases
                            if (connectionHandler.IsRetriableError ex) || not (producerCreatedTsc.TrySetException ex)  then
                                // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                                // still within the initial timeout budget and we are dealing with a retriable error
                                connectionHandler.ReconnectLater ex
                            else
                                // unable to create new consumer, fail operation
                                connectionHandler.Failed()
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)

                | ProducerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    let clientCnx = clientCnx :?> ClientCnx
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveProducer(producerId)

                | ProducerMessage.ConnectionFailed  ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > createProducerTimeout && producerCreatedTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()

                | ProducerMessage.BeginSendMessage (message, channel) ->

                    Log.Logger.LogDebug("{0} BeginSendMessage", prefix)
                    let sequenceId = Generators.getNextSequenceId()
                    let metadata =
                        MessageMetadata (
                            SequenceId = %sequenceId,
                            PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                            ProducerName = producerConfig.ProducerName,
                            UncompressedSize = (message.Length |> uint32)
                        )
                    let payload = Commands.newSend producerId sequenceId 1 metadata message
                    let tcs = TaskCompletionSource()
                    this.Mb.Post(SendMessage { SequenceId = sequenceId; Payload = payload; Tcs = tcs })
                    channel.Reply(tcs)

                | ProducerMessage.SendMessage pendingMessage ->

                    Log.Logger.LogDebug("{0} SendMessage id={1}", prefix, %pendingMessage.SequenceId)
                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let! success = clientCnx.Send pendingMessage.Payload
                            if success then
                                Log.Logger.LogDebug("{0} send complete", prefix)
                            else
                                pendingMessage.Tcs.SetException(ConnectionFailedOnSend "SendMessage")
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    else
                        pendingMessage.Tcs.SetException(ProducerException "Producer send queue is full.")

                | ProducerMessage.SendReceipt receipt ->

                    let sequenceId = %receipt.SequenceId
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = pendingMessage.SequenceId
                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning(
                            "{0} Got ack for message. Expecting: {1} - got: {2} - queue-size: {3}",
                            prefix, expectedSequenceId, sequenceId, pendingMessages.Count)
                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Close()
                        | _ -> ()
                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogDebug(
                            "{0} Got ack for timed out message {1} last-seq: {2}",
                            prefix, sequenceId, expectedSequenceId)
                    else
                        Log.Logger.LogDebug(
                            "{0} Received ack for message {1}",
                            prefix, sequenceId)
                        pendingMessage.Tcs.SetResult(MessageId.FromMessageIdData(receipt.MessageId))
                        pendingMessages.Dequeue() |> ignore

                | ProducerMessage.SendError error ->

                    Log.Logger.LogError("{0} SendError {1}", prefix, error.Message)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx -> clientCnx.Close()
                    | _ -> ()

                | ProducerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        // TODO failPendingReceive
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseProducer producerId requestId
                        task {
                            try
                                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                                response |> PulsarResponseType.GetEmpty
                                clientCnx.RemoveProducer(producerId)
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
                return! loop ()
            }
        loop ()
    )

    member __.SendAndWaitAsync (msg: byte[]) =
        task {
            let! tcs = mb.PostAndAsyncReply(fun channel -> BeginSendMessage (msg, channel))
            return! tcs.Task
        }

    member __.SendAsync (msg: byte[]) =
        task {
            mb.PostAndAsyncReply(fun channel -> BeginSendMessage (msg, channel)) |> ignore
        }

    member __.CloseAsync() =
        task {
            let! result = mb.PostAndAsyncReply(ProducerMessage.Close)
            return! result
        }

    member private __.InitInternal() =
       task {
           do connectionHandler.GrabCnx()
           return! producerCreatedTsc.Task
       }

    member private __.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = new Producer(producerConfig, lookup)
            return! producer.InitInternal()
        }
