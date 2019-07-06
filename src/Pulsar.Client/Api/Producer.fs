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
}

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
    let producerId = Generators.getNextProducerId()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let partitionIndex = -1

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->
        let rec loop (state: ProducerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ProducerMessage.Connect ((broker, mb), channel) ->
                    if state.Connection = NotConnected
                    then
                        let! connection = SocketManager.registerProducer broker producerConfig producerId mb |> Async.AwaitTask
                        channel.Reply()
                        return! loop { state with Connection = Connected connection }
                    else
                        return! loop state
                | ProducerMessage.Reconnect mb ->
                    // TODO backoff
                    if state.Connection = NotConnected
                    then
                        try
                            let! broker = lookup.GetBroker(producerConfig.Topic) |> Async.AwaitTask
                            let! connection = SocketManager.reconnectProducer broker producerId |> Async.AwaitTask
                            return! loop { state with Connection = Connected connection }
                        with
                        | ex ->
                            mb.Post(ProducerMessage.Reconnect mb)
                            Log.Logger.LogError(ex, "Error reconnecting")
                            return! loop state
                    else
                        return! loop state
                | ProducerMessage.Disconnected (connection, mb) ->
                    if state.Connection = Connected connection
                    then
                        mb.Post(ProducerMessage.Reconnect mb)
                        return! loop { state with Connection = NotConnected }
                    else
                        return! loop state
                | ProducerMessage.SendMessage (payload, channel) ->
                    match state.Connection with
                    | Connected conn ->
                        do! SocketManager.send (conn, payload)
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
                | ProducerMessage.ProducerClosed mb ->
                    let! broker = lookup.GetBroker(producerConfig.Topic) |> Async.AwaitTask
                    let! newConnection = SocketManager.registerProducer broker producerConfig producerId mb |> Async.AwaitTask
                    return! loop { state with Connection = Connected newConnection }
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
        loop { Connection = NotConnected }
    )

    member this.SendAndWaitAsync (msg: byte[]) =
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

    member this.SendAsync (msg: byte[]) =
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
        task {
            let! broker = lookup.GetBroker(producerConfig.Topic)
            return! mb.PostAndAsyncReply(fun channel -> ProducerMessage.Connect ((broker, mb), channel))
        }

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = Producer(producerConfig, lookup)
            do! producer.InitInternal()
            return producer
        }
