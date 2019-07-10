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

type ProducerState = {
    Connection: ConnectionState
    ReconnectCount: int
}

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
        }

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->
        let rec loop (state: ProducerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ProducerMessage.Connect channel ->
                    if state.Connection = NotConnected
                    then
                        let! clientCnx = registerProducer() |> Async.AwaitTask
                        channel.Reply()
                        return! loop { state with Connection = Connected clientCnx }
                    else
                        return! loop state
                | ProducerMessage.Reconnect ->
                    // TODO backoff
                    if state.Connection = NotConnected
                    then
                        try
                            let! clientCnx = registerProducer() |> Async.AwaitTask
                            return! loop { state with Connection = Connected clientCnx; ReconnectCount = 0 }
                        with
                        | ex ->
                            this.Mb.Post(ProducerMessage.Reconnect)
                            Log.Logger.LogError(ex, "Error reconnecting")
                            if state.ReconnectCount > 3
                            then
                                raise ex
                            else
                                return! loop { state with ReconnectCount = state.ReconnectCount + 1 }
                    else
                        return! loop state
                | ProducerMessage.Disconnected ->
                    this.Mb.Post(ProducerMessage.Reconnect)
                    return! loop { state with Connection = NotConnected }
                | ProducerMessage.SendMessage (payload, channel) ->
                    match state.Connection with
                    | Connected clientCnx ->
                        do! clientCnx.Send payload
                        channel.Reply()
                        return! loop state
                    | NotConnected ->
                        //TODO put message on schedule
                        Log.Logger.LogWarning("NotConnected, skipping send")
                        return! loop state
                | ProducerMessage.SendReceipt receipt ->
                    let sequenceId = %receipt.SequenceId
                    match messages.TryGetValue(sequenceId) with
                    | true, tsc ->
                        tsc.SetResult(MessageId.FromMessageIdData(receipt.MessageId))
                        messages.TryRemove(sequenceId) |> ignore
                    | false, _ -> ()
                    return! loop state
                | ProducerMessage.SendError error ->
                    let sequenceId = %error.SequenceId
                    match messages.TryGetValue(sequenceId) with
                    | true, tsc ->
                        Log.Logger.LogError("SendError code: {0} message: {1}", error.Error, error.Message)
                        tsc.SetException(Exception(error.Message))
                        messages.TryRemove(sequenceId) |> ignore
                    | false, _ -> ()
                    return! loop state
            }
        loop { Connection = NotConnected; ReconnectCount = 0 }
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
