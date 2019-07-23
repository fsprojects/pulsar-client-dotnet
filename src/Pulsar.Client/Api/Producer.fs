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

    let connectionOpened() =
        this.Mb.Post(ProducerMessage.ConnectionOpened)

    let connectionHandler = ConnectionHandler(lookup, producerConfig.Topic.CompleteTopicName, connectionOpened)
    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->

        let pendingMessages = Queue<PendingMessage>()

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with

                | ProducerMessage.ConnectionOpened ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.RegisterProducer producerConfig producerId this.Mb |> Async.AwaitTask
                        // process pending messages
                        if pendingMessages.Count > 0 then
                            Log.Logger.LogInformation("Resending {0} pending messages", pendingMessages.Count)
                            let mutable sentCount = 0

                            while pendingMessages.Count > 0 do
                                let pendingMessage = pendingMessages.Dequeue()
                                this.Mb.Post(SendMessage pendingMessage)
                                sentCount <- sentCount + 1

                            Log.Logger.LogInformation("{0} pending messages was sent", sentCount)
                    | _ ->
                        Log.Logger.LogWarning("Connection opened but connection is not ready")

                | ProducerMessage.ConnectionClosed ->
                    do! connectionHandler.ConnectionClosed()

                | ProducerMessage.BeginSendMessage (message, channel) ->

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

                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                    else
                        raise <| ProducerException("Producer send queue is full.")

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.Send pendingMessage.Payload
                        Log.Logger.LogDebug("Send complete")
                    | _ ->
                        Log.Logger.LogWarning("NotConnected, skipping send")

                | ProducerMessage.SendReceipt receipt ->

                    let sequenceId = %receipt.SequenceId
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = pendingMessage.SequenceId

                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning(
                            "[{0}] [{1}] Got ack for message. Expecting: {2} - got: {3} - queue-size: {4}",
                            producerConfig.Topic, producerConfig.ProducerName, expectedSequenceId, sequenceId, pendingMessages.Count)

                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Close()
                        | _ -> ()

                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogDebug(
                            "[{0}] [{1}] Got ack for timed out message {2} last-seq: {3}",
                            producerConfig.Topic, producerConfig.ProducerName, sequenceId, expectedSequenceId)
                    else
                        Log.Logger.LogDebug(
                            "[{0}] [{1}] Received ack for message {2}",
                            producerConfig.Topic, producerConfig.ProducerName, sequenceId)

                        pendingMessage.Tcs.SetResult(MessageId.FromMessageIdData(receipt.MessageId))
                        pendingMessages.Dequeue() |> ignore

                | ProducerMessage.SendError error ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx -> clientCnx.Close()
                    | _ -> ()

                | ProducerMessage.Close channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        //TODO check if we should block mb on closing
                        connectionHandler.Closing()
                        // TODO failPendingReceive
                        Log.Logger.LogInformation("Starting close producer {0}", producerId)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseProducer producerId requestId
                        let result = clientCnx.SendAndWaitForReply requestId payload
                        let newTask =
                            result.ContinueWith(
                                Action<Task<PulsarTypes>>(
                                    fun t ->
                                        if t.Status = TaskStatus.RanToCompletion then
                                            match t.Result with
                                                | Empty ->
                                                    clientCnx.RemoveProducer(producerId)
                                                    connectionHandler.Closed()
                                                    Log.Logger.LogInformation("Producer {0} closed", producerId)
                                                | _ ->
                                                    // TODO: implement correct error handling
                                                    failwith "Incorrect return type"
                                        else
                                            Log.Logger.LogError(t.Exception, "Failed to close producer: {0}", producerId)
                                )
                            )
                        channel.Reply(newTask)
                    | _ ->
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
        Log.Logger.LogDebug("Sending Async")
        task {
            mb.PostAndAsyncReply(fun channel -> BeginSendMessage (msg, channel)) |> ignore
        }

    member __.CloseAsync() =
        task {
            let! result = mb.PostAndAsyncReply(ProducerMessage.Close)
            return! result
        }

    member private __.InitInternal() =
       connectionHandler.Connect()

    member private __.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        let producer = new Producer(producerConfig, lookup)
        producer.InitInternal()
        producer
