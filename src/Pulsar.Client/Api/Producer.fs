namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Collections.Concurrent

type ProducerException(message) =
    inherit Exception(message)

type ProducerState = {
    Connection: ConnectionState
}

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =    
    let producerId = Generators.getNextProducerId()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageIdData>>()
    let partitionIndex = -1

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->
        let rec loop (state: ProducerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ProducerMessage.Connect ((broker, mb), channel) ->
                    let! connection = SocketManager.registerProducer broker producerId mb |> Async.AwaitTask
                    channel.Reply()
                    return! loop { state with Connection = Connected connection }
                | ProducerMessage.Reconnect ->
                    // TODO backoff
                    let topicName = TopicName(producerConfig.Topic)
                    let! broker = lookup.GetBroker(topicName) |> Async.AwaitTask
                    let! connection = SocketManager.getConnection broker |> Async.AwaitTask
                    return! loop { state with Connection = Connected connection }
                | ProducerMessage.Disconnected (connection, mb) ->
                    if state.Connection = Connected connection
                    then
                        mb.Post(ProducerMessage.Reconnect)
                        return! loop { state with Connection = NotConnected }
                    else 
                        return! loop state
                | ProducerMessage.SendMessage (payload, channel) ->
                    match state.Connection with
                    | Connected conn ->
                        let! flushResult = SocketManager.send (conn, payload)
                        channel.Reply(flushResult)
                        return! loop state
                    | NotConnected ->
                        //TODO put message on schedule
                        return! loop state
                | ProducerMessage.SendReceipt receipt ->
                    let sequenceId = %receipt.SequenceId
                    match messages.TryGetValue(sequenceId) with
                    | true, tsc ->
                        tsc.SetResult(receipt.MessageId)
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
                Commands.newSend producerId sequenceId 1 ChecksumType.No metadata payload
                |> ReadOnlyMemory<byte>
            let! flushResult = mb.PostAndAsyncReply(fun channel -> SendMessage (command, channel))
            let tsc = TaskCompletionSource<MessageIdData>()
            if messages.TryAdd(sequenceId, tsc)
            then
                return! tsc.Task
            else 
                return failwith "Unable to add tsc"
        }

    member this.SendAsync (msg: byte[]) =
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
                Commands.newSend producerId sequenceId 1 ChecksumType.No metadata payload
                |> ReadOnlyMemory<byte>
            return! mb.PostAndAsyncReply(fun channel -> SendMessage (command, channel))
        }    

    member private __.InitInternal() =
        task {
            let topicName = TopicName(producerConfig.Topic)
            let! broker = lookup.GetBroker(topicName)
            return! mb.PostAndAsyncReply(fun channel -> ProducerMessage.Connect ((broker, mb), channel))
        }

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = Producer(producerConfig, lookup)
            do! producer.InitInternal()
            return producer
        }
        