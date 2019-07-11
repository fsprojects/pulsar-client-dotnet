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

type ProducerException(message) =
    inherit Exception(message)

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) as this =
    let producerId = Generators.getNextProducerId()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let partitionIndex = -1

    let registerProducer() =
        task {
            let! broker = lookup.GetBroker(producerConfig.Topic)
            let! clientCnx = ConnectionPool.getConnection broker
            do! clientCnx.RegisterProducer producerConfig producerId this.Mb
            return clientCnx
        } |> Async.AwaitTask

    let connectionHandler = ConnectionHandler registerProducer

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ProducerMessage.Connect channel ->
                    do! connectionHandler.Connect()
                    channel.Reply()
                | ProducerMessage.ConnectionClosed ->
                    do! connectionHandler.ConnectionClosed()
                | ProducerMessage.SendMessage (payload, channel) ->
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        do! clientCnx.Send payload
                        channel.Reply()
                    | _ ->
                        //TODO put message on schedule
                        Log.Logger.LogWarning("NotConnected, skipping send")
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

    member private __.InitInternal() =
        mb.PostAndAsyncReply(fun channel -> ProducerMessage.Connect channel)

    member private __.Mb with get() = mb

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = Producer(producerConfig, lookup)
            do! producer.InitInternal()
            return producer
        }
