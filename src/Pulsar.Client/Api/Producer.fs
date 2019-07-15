namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Collections.Concurrent
open Microsoft.Extensions.Logging
open System.Collections.Generic

type ProducerException(message) =
    inherit Exception(message)

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) as this =
    let producerId = Generators.getNextProducerId()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let pendingMessages = Queue<Payload>()
    let partitionIndex = -1

    let connectionOpened() =
        this.Mb.Post(ProducerMessage.ConnectionOpened)

    let connectionHandler = ConnectionHandler(lookup, producerConfig.Topic, connectionOpened)

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ProducerMessage.ConnectionOpened ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.RegisterProducer producerConfig producerId this.Mb |> Async.AwaitTask
                        // process pending messages
                        if pendingMessages.Count > 0
                        then
                            Log.Logger.LogInformation("Resending {0} pending messages", pendingMessages.Count)
                            let mutable sentCount = 0
                            while pendingMessages.Count > 0 do
                                let payload = pendingMessages.Dequeue()
                                do! clientCnx.Send payload
                                sentCount <- sentCount + 1
                            Log.Logger.LogInformation("{0} pending messages was sent", sentCount)
                    | _ ->
                        Log.Logger.LogWarning("Connection opened but connection is not ready")
                | ProducerMessage.ConnectionClosed ->
                    do! connectionHandler.ConnectionClosed()
                | ProducerMessage.SendMessage (payload, channel) ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.Send payload
                    | _ ->
                        Log.Logger.LogWarning("NotConnected, skipping send")
                        if pendingMessages.Count < producerConfig.MaxPendingMessages
                        then
                            pendingMessages.Enqueue payload
                        else
                            raise <| ProducerException("Producer send queue is full.")
                    channel.Reply()
                | ProducerMessage.SendReceipt receipt ->
                    let sequenceId = %receipt.SequenceId
                    match messages.TryGetValue(sequenceId) with
                    | true, tsc ->
                        tsc.SetResult(MessageId.FromMessageIdData(receipt.MessageId))
                        messages.TryRemove(sequenceId) |> ignore
                    | false, _ -> ()
                | ProducerMessage.SendError error ->
                    let sequenceId = %error.SequenceId
                    match messages.TryGetValue(sequenceId) with
                    | true, tsc ->
                        Log.Logger.LogError("SendError code: {0} message: {1}", error.Error, error.Message)
                        tsc.SetException(Exception(error.Message))
                        messages.TryRemove(sequenceId) |> ignore
                    | false, _ -> ()
                | ProducerMessage.Close channel ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        do! clientCnx.UnregisterProducer producerId |> Async.AwaitTask
                        connectionHandler.Closed()
                    | _ ->
                        connectionHandler.Closed()
                    channel.Reply()
                return! loop ()
            }
        loop ()
    )

    member __.SendAndWaitAsync (msg: byte[]) =
        task {
            let payload = msg;
            let sequenceId = Generators.getNextSequenceId()
            let metadata =
                MessageMetadata (
                    SequenceId = %sequenceId,
                    PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                    ProducerName = producerConfig.ProducerName,
                    UncompressedSize = (payload.Length |> uint32)
                )
            let command =
                Commands.newSend producerId sequenceId 1 metadata payload
            do! mb.PostAndAsyncReply(fun channel -> SendMessage (command, channel))
            let tsc = TaskCompletionSource()
            if messages.TryAdd(sequenceId, tsc)
            then
                return! tsc.Task
            else
                return failwith "Unable to add tsc"
        }

    member __.SendAsync (msg: byte[]) =
        Log.Logger.LogDebug("Sending Async")

        task {
            let payload = msg;
            let sequenceId = Generators.getNextSequenceId()
            let metadata =
                MessageMetadata (
                    SequenceId = %sequenceId,
                    PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                    ProducerName = producerConfig.ProducerName,
                    UncompressedSize = (payload.Length |> uint32)
                )
            let command =
                Commands.newSend producerId sequenceId 1 metadata payload
            return! mb.PostAndAsyncReply(fun channel -> SendMessage (command, channel))
        }

    member __.CloseAsync() =
        mb.PostAndAsyncReply(ProducerMessage.Close)

    member private __.InitInternal() =
       connectionHandler.Connect()

    member private __.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        let producer = new Producer(producerConfig, lookup)
        producer.InitInternal()
        producer
