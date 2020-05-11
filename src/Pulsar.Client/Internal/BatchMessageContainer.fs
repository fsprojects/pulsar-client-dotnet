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

    let makeBatch<'T> (batchItems: BatchItem<'T> seq) =
        use messageStream = MemoryStreamManager.GetStream()
        use messageWriter = new BinaryWriter(messageStream)
        let batchCallbacks =
            batchItems
            |> Seq.mapi (fun index batchItem ->
                let message = batchItem.Message
                let smm = SingleMessageMetadata(PayloadSize = message.Payload.Length)
                match message.Key with
                | Some key ->
                    smm.PartitionKey <- %key.PartitionKey
                    smm.PartitionKeyB64Encoded <- key.IsBase64Encoded
                | _ ->
                    ()
                if message.Properties.Count > 0 then
                    for property in message.Properties do
                        smm.Properties.Add(KeyValue(Key = property.Key, Value = property.Value))
                Serializer.SerializeWithLengthPrefix(messageStream, smm, PrefixStyle.Fixed32BigEndian)
                messageWriter.Write(message.Payload)
                (BatchDetails(%index, BatchMessageAcker.NullAcker), message, batchItem.Tcs))
            |> Seq.toArray
        let batchPayload = messageStream.ToArray()
        (batchPayload, batchCallbacks)

[<AbstractClass>]
type internal MessageContainer<'T>(config: ProducerConfiguration) =

    let maxBytesInBatch = config.BatchingMaxBytes
    let maxNumMessagesInBatch = config.BatchingMaxMessages

    abstract member Add: BatchItem<'T> -> bool
    member this.HaveEnoughSpace (msgBuilder: MessageBuilder<'T>) =
        let messageSize = msgBuilder.Payload.Length
        ((maxBytesInBatch <= 0 && (messageSize + this.CurrentBatchSizeBytes) <= this.MaxMessageSize)
            || (maxBytesInBatch > 0 && (messageSize + this.CurrentBatchSizeBytes) <= maxBytesInBatch)
        ) && (maxNumMessagesInBatch <= 0 || this.NumMessagesInBatch < maxNumMessagesInBatch)
    member this.IsBatchFull() =
        (maxBytesInBatch > 0 && this.CurrentBatchSizeBytes >= maxBytesInBatch)
        || (maxBytesInBatch <= 0 && this.CurrentBatchSizeBytes >= this.MaxMessageSize)
        || (maxNumMessagesInBatch > 0 && this.NumMessagesInBatch >= maxNumMessagesInBatch)
    abstract member CreateOpSendMsg: unit -> byte[] * BatchCallback<'T>[]
    abstract member CreateOpSendMsgs: unit -> seq<byte[] * BatchCallback<'T>[]>
    abstract member Clear: unit -> unit
    abstract member IsMultiBatches: bool
    abstract member Discard: exn -> unit
    member val CurrentBatchSizeBytes = 0 with get, set
    member val MaxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE with get, set
    member val NumMessagesInBatch = 0 with get, set


open BatchHelpers
open System.Collections.Generic
open Microsoft.Extensions.Logging

type internal DefaultBatchMessageContainer<'T>(prefix: string, config: ProducerConfiguration) =
    inherit MessageContainer<'T>(config)

    let prefix = prefix + " DefaultBatcher"
    let batchItems = ResizeArray<BatchItem<'T>>()

    override this.Add batchItem =
        Log.Logger.LogDebug("{0} add message to batch, num messages in batch so far is {1}", prefix, this.NumMessagesInBatch)
        this.CurrentBatchSizeBytes <- this.CurrentBatchSizeBytes + batchItem.Message.Payload.Length
        this.NumMessagesInBatch <- this.NumMessagesInBatch + 1
        batchItems.Add(batchItem)
        this.IsBatchFull()
    override this.CreateOpSendMsg () =
        makeBatch batchItems
    override this.CreateOpSendMsgs () =
        raise <| NotSupportedException()
    override this.Clear() =
        batchItems.Clear()
        this.CurrentBatchSizeBytes <- 0
        this.NumMessagesInBatch <- 0
    override this.IsMultiBatches = false
    override this.Discard ex =
        batchItems |> Seq.iter(fun batchItem -> batchItem.Tcs.SetException(ex))
        this.Clear()

type internal KeyBasedBatchMessageContainer<'T>(prefix: string, config: ProducerConfiguration) =
    inherit MessageContainer<'T>(config)

    let prefix = prefix + " KeyBasedBatcher"
    let keyBatchItems = Dictionary<PartitionKey, ResizeArray<BatchItem<'T>>>()

    override this.Add batchItem =
        Log.Logger.LogDebug("{0} add message to batch, num messages in batch so far is {1}", prefix, this.NumMessagesInBatch)
        this.CurrentBatchSizeBytes <- this.CurrentBatchSizeBytes + batchItem.Message.Payload.Length
        this.NumMessagesInBatch <- this.NumMessagesInBatch + 1
        let key =
            match batchItem.Message.Key with
            | Some key -> key.PartitionKey
            | None -> %""
        match keyBatchItems.TryGetValue key with
        | true, items ->
            items.Add(batchItem)
        | false, _ ->
            let arr = ResizeArray<BatchItem<'T>>()
            arr.Add(batchItem)
            keyBatchItems.Add(key, arr)
        this.IsBatchFull()
    override this.CreateOpSendMsg () =
        raise <| NotSupportedException()
    override this.CreateOpSendMsgs () =
        keyBatchItems.Values
        |> Seq.map (fun batchItems -> makeBatch batchItems)
    override this.Clear() =
        keyBatchItems.Clear()
        this.CurrentBatchSizeBytes <- 0
        this.NumMessagesInBatch <- 0
    override this.IsMultiBatches = true
    override this.Discard ex =
        keyBatchItems.Values |> Seq.iter(fun batchItems ->
            batchItems |> Seq.iter (fun batchItem ->
                batchItem.Tcs.SetException(ex)
            ))
        this.Clear()
