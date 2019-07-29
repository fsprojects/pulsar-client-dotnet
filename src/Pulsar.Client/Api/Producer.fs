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

    let prefix = sprintf "producer(%u, %s)" %producerId producerConfig.ProducerName

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

                        Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                        clientCnx.AddProducer producerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        let payload =
                            Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName producerId requestId
                        let! success =
                            fun () -> clientCnx.SendAndWaitForReply requestId payload
                            |> PulsarTypes.GetProducerSuccess
                            |> Async.AwaitTask
                        Log.Logger.LogInformation("{0} registered with name {1}", prefix, success.GeneratedProducerName)

                        // process pending messages
                        if pendingMessages.Count > 0 then
                            Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
                            while pendingMessages.Count > 0 do
                                let pendingMessage = pendingMessages.Dequeue()
                                this.Mb.Post(SendMessage pendingMessage)
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)

                | ProducerMessage.ConnectionClosed ->

                    Log.Logger.LogInformation("{0} ConnectionClosed", prefix)
                    do! connectionHandler.ConnectionClosed()

                | ProducerMessage.BeginSendMessage (message, channel) ->

                    Log.Logger.LogInformation("{0} BeginSendMessage", prefix)
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

                    Log.Logger.LogInformation("{0} SendMessage id={1}", prefix, %pendingMessage.SequenceId)
                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                    else
                        raise <| ProducerException("Producer send queue is full.")

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.Send pendingMessage.Payload
                        Log.Logger.LogDebug("{0} send complete", prefix)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)

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
                                do!
                                    fun () -> clientCnx.SendAndWaitForReply requestId payload
                                    |> PulsarTypes.GetEmpty
                                    |> Async.AwaitTask

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
       connectionHandler.Connect()

    member private __.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        let producer = new Producer(producerConfig, lookup)
        producer.InitInternal()
        producer
