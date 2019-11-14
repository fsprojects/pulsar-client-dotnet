namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open Pulsar.Client.Api
open System.IO
open pulsar.proto
open FSharp.UMX
open System
open ProtoBuf

module internal BatchHelpers =
    [<Literal>]
    let INITIAL_BATCH_BUFFER_SIZE = 1024
    [<Literal>]
    let MAX_MESSAGE_BATCH_SIZE_BYTES = 131072 //128 * 1024

    let makeBatch batchItems =
        use messageStream = MemoryStreamManager.GetStream()
        use messageWriter = new BinaryWriter(messageStream)
        let batchCallbacks =
            batchItems
            |> Seq.mapi (fun index batchItem ->
                let message = batchItem.Message
                let smm = SingleMessageMetadata(PayloadSize = message.Value.Length)
                if String.IsNullOrEmpty(%message.Key) |> not then
                    smm.PartitionKey <- %message.Key
                if message.Properties.Count > 0 then
                    for property in message.Properties do
                        smm.Properties.Add(KeyValue(Key = property.Key, Value = property.Value))
                Serializer.SerializeWithLengthPrefix(messageStream, smm, PrefixStyle.Fixed32BigEndian)
                messageWriter.Write(message.Value)
                ({ MessageId.Earliest with Type = Cumulative(%index, BatchMessageAcker.NullAcker) }), batchItem.Tcs)
            |> Seq.toArray
        let batchPayload = messageStream.ToArray()
        (batchPayload, batchCallbacks)

type internal IBatchMessageContainer =
    abstract member Add: BatchItem -> unit
    abstract member GetNumMessagesInBatch: unit -> int
    abstract member GetCurrentBatchSize: unit -> int
    abstract member HaveEnoughSpace: MessageBuilder -> bool
    abstract member CreateOpSendMsg: unit -> byte[] * BatchCallback[]
    abstract member CreateOpSendMsgs: unit -> seq<byte[] * BatchCallback[]>
    abstract member Clear: unit -> unit
    abstract member IsMultiBatches: bool
    abstract member Discard: exn -> unit

open BatchHelpers
open System.Collections.Generic
open Microsoft.Extensions.Logging

type internal DefaultBatchMessageContainer(prefix: string, config: ProducerConfiguration) =
    let prefix = prefix + " DefaultBatcher"
    let mutable currentBatchSizeBytes = 0
    let batchItems = ResizeArray<BatchItem>()

    interface IBatchMessageContainer with
        member this.Add batchItem =
            Log.Logger.LogDebug("{0} add message to batch, num messages in batch so far is {1}", prefix, batchItems.Count)
            currentBatchSizeBytes <- currentBatchSizeBytes + batchItem.Message.Value.Length
            batchItems.Add(batchItem)
        member this.GetNumMessagesInBatch () =
            batchItems.Count
        member this.GetCurrentBatchSize () =
            currentBatchSizeBytes
        member this.HaveEnoughSpace msgBuilder =
            let messageSize = msgBuilder.Value.Length
            messageSize + currentBatchSizeBytes <= MAX_MESSAGE_BATCH_SIZE_BYTES
                && batchItems.Count < config.BatchingMaxMessages
        member this.CreateOpSendMsg () =
            makeBatch batchItems
        member this.CreateOpSendMsgs () =
            raise <| NotSupportedException()
        member this.Clear() =
            batchItems.Clear()
            currentBatchSizeBytes <- 0
        member this.IsMultiBatches = false
        member this.Discard ex =
            batchItems |> Seq.iter(fun batchItem -> batchItem.Tcs.SetException(ex))
            (this :> IBatchMessageContainer).Clear()

type internal KeyBasedBatchMessageContainer(prefix: string, config: ProducerConfiguration) =
    let prefix = prefix + " KeyBasedBatcher"
    let mutable currentBatchSizeBytes = 0
    let mutable numMessagesInBatch = 0
    let keyBatchItems = Dictionary<MessageKey, ResizeArray<BatchItem>>()

    interface IBatchMessageContainer with
        member this.Add batchItem =
            Log.Logger.LogDebug("{0} add message to batch, num messages in batch so far is {1}", prefix, numMessagesInBatch)
            currentBatchSizeBytes <- currentBatchSizeBytes + batchItem.Message.Value.Length
            numMessagesInBatch <- numMessagesInBatch + 1
            match keyBatchItems.TryGetValue batchItem.Message.Key with
            | true, items ->
                items.Add(batchItem)
            | false, _ ->
                let arr = ResizeArray<BatchItem>()
                arr.Add(batchItem)
                keyBatchItems.Add(batchItem.Message.Key, arr)
        member this.GetNumMessagesInBatch () =
            numMessagesInBatch
        member this.GetCurrentBatchSize () =
            currentBatchSizeBytes
        member this.HaveEnoughSpace msgBuilder =
            let messageSize = msgBuilder.Value.Length
            messageSize + currentBatchSizeBytes <= MAX_MESSAGE_BATCH_SIZE_BYTES
                && numMessagesInBatch < config.BatchingMaxMessages
        member this.CreateOpSendMsg () =
            raise <| NotSupportedException()
        member this.CreateOpSendMsgs () =
            keyBatchItems.Values
            |> Seq.map (fun batchItems -> makeBatch batchItems)
        member this.Clear() =
            keyBatchItems.Clear()
            currentBatchSizeBytes <- 0
            numMessagesInBatch <- 0
        member this.IsMultiBatches = true
        member this.Discard ex =
            keyBatchItems.Values |> Seq.iter(fun batchItems ->
                batchItems |> Seq.iter (fun batchItem ->
                    batchItem.Tcs.SetException(ex)
                ))
            (this :> IBatchMessageContainer).Clear()