namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open Pulsar.Client.Api
open System.IO
open Pulsar.Client.Transaction
open pulsar.proto
open FSharp.UMX
open System
open ProtoBuf
open System.Collections.Generic
open Microsoft.Extensions.Logging

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
                let smm = SingleMessageMetadata(
                           PayloadSize = message.Payload.Length,
                           SequenceId = (batchItem.SequenceId |> uint64)
                       )
                match message.Key with
                | Some key ->
                    smm.PartitionKey <- %key.PartitionKey
                    smm.PartitionKeyB64Encoded <- key.IsBase64Encoded
                | _ ->
                    ()
                match message.OrderingKey with
                | Some orderingKey ->
                    smm.OrderingKey <- orderingKey
                | _ ->
                    ()
                match message.EventTime with
                | Some eventTime ->
                    smm.EventTime <- eventTime |> uint64
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

type internal OpSendMsg<'T> = byte[] * BatchCallback<'T>[]
type internal OpSendMsgWrapper<'T> = {
    OpSendMsg: OpSendMsg<'T>
    LowestSequenceId: SequenceId
    HighestSequenceId: SequenceId
    PartitionKey: MessageKey option
    OrderingKey: byte[] option
    TxnId: TxnId option
    ReplicationClusters: IEnumerable<string> option
}

[<AbstractClass>]
type internal MessageContainer<'T>(config: ProducerConfiguration) =

    let maxBytesInBatch = config.BatchingMaxBytes
    let maxNumMessagesInBatch = config.BatchingMaxMessages

    abstract member Add: BatchItem<'T> -> bool
    member this.AddStart (prefix, batchItem) =
        Log.Logger.LogDebug("{0} add message to batch, num messages in batch so far is {1}", prefix, this.NumMessagesInBatch)
        this.CurrentBatchSizeBytes <- this.CurrentBatchSizeBytes + batchItem.Message.Payload.Length
        this.NumMessagesInBatch <- this.NumMessagesInBatch + 1
        match this.CurrentTxnId, batchItem.Message.Txn with
        | None, Some txn ->
            this.CurrentTxnId <- Some txn.Id
        | _ ->
            ()

    member this.HaveEnoughSpace (msgBuilder: MessageBuilder<'T>) =
        let messageSize = msgBuilder.Payload.Length
        ((maxBytesInBatch <= 0 && (messageSize + this.CurrentBatchSizeBytes) <= this.MaxMessageSize)
            || (maxBytesInBatch > 0 && (messageSize + this.CurrentBatchSizeBytes) <= maxBytesInBatch)
        ) && (maxNumMessagesInBatch <= 0 || this.NumMessagesInBatch < maxNumMessagesInBatch)
    member this.IsBatchFull() =
        (maxBytesInBatch > 0 && this.CurrentBatchSizeBytes >= maxBytesInBatch)
        || (maxBytesInBatch <= 0 && this.CurrentBatchSizeBytes >= this.MaxMessageSize)
        || (maxNumMessagesInBatch > 0 && this.NumMessagesInBatch >= maxNumMessagesInBatch)
    member this.HasSameTxn (msgBuilder: MessageBuilder<'T>) =
        match msgBuilder.Txn, this.CurrentTxnId with
        | Some txn, Some currentTxnId ->
            txn.Id = currentTxnId
        | _ ->
            true
    abstract member CreateOpSendMsg: unit -> OpSendMsgWrapper<'T>
    abstract member CreateOpSendMsgs: unit -> seq<OpSendMsgWrapper<'T>>
    abstract member Clear: unit -> unit
    abstract member IsMultiBatches: bool
    abstract member Discard: exn -> unit
    member val CurrentBatchSizeBytes = 0 with get, set
    member val MaxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE with get, set
    member val NumMessagesInBatch = 0 with get, set
    member val CurrentTxnId = None with get, set

open BatchHelpers

type internal DefaultBatchMessageContainer<'T>(prefix: string, config: ProducerConfiguration) =
    inherit MessageContainer<'T>(config)

    let prefix = prefix + " DefaultBatcher"
    let batchItems = ResizeArray<BatchItem<'T>>()

    override this.Add batchItem =
        this.AddStart(prefix, batchItem)
        batchItems.Add(batchItem)
        this.IsBatchFull()
    override this.CreateOpSendMsg () =
        let lowestSequenceId = batchItems.[0].SequenceId
        let highestSequenceId = batchItems.[batchItems.Count - 1].SequenceId
        {
            OpSendMsg = makeBatch batchItems
            LowestSequenceId = lowestSequenceId
            HighestSequenceId = highestSequenceId
            PartitionKey = batchItems.[0].Message.Key
            OrderingKey = batchItems.[0].Message.OrderingKey
            TxnId = this.CurrentTxnId
            ReplicationClusters = batchItems.[0].Message.ReplicationClusters
        }
    override this.CreateOpSendMsgs () =
        raise <| NotSupportedException()
    override this.Clear() =
        batchItems.Clear()
        this.CurrentBatchSizeBytes <- 0
        this.NumMessagesInBatch <- 0
        this.CurrentTxnId <- None
    override this.IsMultiBatches = false
    override this.Discard ex =
        batchItems |> Seq.iter(fun batchItem -> batchItem.Tcs |> Option.iter (fun tcs -> tcs.SetException ex))
        this.Clear()

type internal KeyBasedBatchMessageContainer<'T>(prefix: string, config: ProducerConfiguration) =
    inherit MessageContainer<'T>(config)

    let prefix = prefix + " KeyBasedBatcher"
    let keyBatchItems = Dictionary<MessageKey, ResizeArray<BatchItem<'T>>>()

    let getKey (msg: MessageBuilder<'T>) =
        match msg.OrderingKey with
        | Some orderingKey ->
            {
                PartitionKey = %Convert.ToBase64String(orderingKey)
                IsBase64Encoded = true
            }
        | None ->
            match msg.Key with
            | Some key -> key
            | None ->
                {
                    PartitionKey = %""
                    IsBase64Encoded = false
                }


    override this.Add batchItem =
        this.AddStart(prefix, batchItem)
        let key = batchItem.Message |> getKey
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
        keyBatchItems
        |> Seq.map (fun (KeyValue(_, batchItems)) ->
            let lowestSequenceId = batchItems.[0].SequenceId
            let highestSequenceId = batchItems.[batchItems.Count - 1].SequenceId
            {
                OpSendMsg = makeBatch batchItems
                LowestSequenceId = lowestSequenceId
                HighestSequenceId = highestSequenceId
                PartitionKey = batchItems.[0].Message.Key
                OrderingKey = batchItems.[0].Message.OrderingKey
                TxnId = this.CurrentTxnId
                ReplicationClusters = batchItems.[0].Message.ReplicationClusters
            })
    override this.Clear() =
        keyBatchItems.Clear()
        this.CurrentBatchSizeBytes <- 0
        this.NumMessagesInBatch <- 0
    override this.IsMultiBatches = true
    override this.Discard ex =
        keyBatchItems.Values |> Seq.iter(fun batchItems ->
            batchItems |> Seq.iter (fun batchItem ->
                batchItem.Tcs |> Option.iter (fun tcs -> tcs.SetException ex)
            ))
        this.Clear()
