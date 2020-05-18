namespace Pulsar.Client.Common

open System.Net
open System
open pulsar.proto
open System.IO.Pipelines
open System.IO
open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Internal
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Text
open ProtoBuf

type internal PartitionedTopicMetadata =
    {
        Partitions: int
    }

type SchemaVersion = SchemaVersion of byte[]

type internal ProducerSuccess =
    {
        GeneratedProducerName: string
        SchemaVersion: SchemaVersion option
        LastSequenceId: int64
    }

type internal LookupTopicResult =
    {
        Proxy: bool
        BrokerServiceUrl : string
        BrokerServiceUrlTls: string
        Redirect: bool
        Authoritative: bool
    }

type internal TopicsOfNamespace =
    {
        Topics : string list
    }    

type KeyValueEncodingType =
    | SEPARATED = 0
    | INLINE = 1

type SchemaType =
    | NONE = 0
    | STRING = 1
    | JSON = 2
    | PROTOBUF = 3
    | AVRO = 4
    | BOOLEAN = 5
    | INT8 = 6
    | INT16 = 7
    | INT32 = 8
    | INT64 = 9
    | FLOAT = 10
    | DOUBLE = 11
    | DATE = 12
    | TIME = 13
    | TIMESTAMP = 14
    | KEY_VALUE = 15
    | BYTES = -1
    | AUTO_CONSUME = -3
    | AUTO_PUBLISH = -4

type SchemaInfo =
    {
        Name: string
        Schema: byte[]
        Type: SchemaType
        Properties: IReadOnlyDictionary<string, string>
    }
    
type internal TopicSchema =
    {
        SchemaInfo: SchemaInfo
        SchemaVersion: SchemaVersion option
    }

type SubscriptionType =
    | Exclusive = 0
    | Shared = 1
    | Failover = 2
    | KeyShared = 3

type SubscriptionInitialPosition =
    | Latest = 0
    | Earliest = 1

type BatchBuilder =
    | Default = 0
    | KeyBased = 1

type internal TopicDomain =
    | Persistent
    | NonPersistent

type SubscriptionMode =
    | Durable = 0
    | NonDurable = 1

type internal AckType =
    | Individual
    | Cumulative
    member this.ToCommandAckType() =
        match this with
        | AckType.Individual -> CommandAck.AckType.Individual
        | AckType.Cumulative -> CommandAck.AckType.Cumulative

type BatchDetails = BatchIndex * BatchMessageAcker

type MessageIdType =
    | Individual
    | Cumulative of BatchDetails

type MessageId =
    {
        LedgerId: LedgerId
        EntryId: EntryId
        Type: MessageIdType
        Partition: int
        TopicName: CompleteTopicName
    }
    with
        static member Earliest =
            {
                LedgerId = %(-1L)
                EntryId = %(-1L)
                Type = Individual
                Partition = %(-1)
                TopicName = %""
            }
        static member Latest =
            {
                LedgerId = %(Int64.MaxValue)
                EntryId = %(Int64.MaxValue)
                Type = Individual
                Partition = %(-1)
                TopicName = %""
            }
        member internal this.PrevBatchMessageId
            with get() = { this with EntryId = this.EntryId - %1L; Type = Individual }
        member this.ToByteArray() =
            let data = MessageIdData(ledgerId = uint64 %this.LedgerId, entryId = uint64 %this.EntryId)
            if this.Partition >= 0 then
                data.Partition <- this.Partition
            match this.Type with
            | Cumulative (batchIndex, _) when %batchIndex >= 0 ->
                data.BatchIndex <- %batchIndex
            | _ ->
                ()
            use stream = MemoryStreamManager.GetStream()
            Serializer.Serialize(stream, data)
            stream.ToArray()
        static member FromByteArray (data: byte[]) =
            use stream = new MemoryStream(data)
            let msgData = Serializer.Deserialize<MessageIdData>(stream)
            if msgData.BatchIndex >= 0 then
                {
                    LedgerId = %(int64 msgData.ledgerId)
                    EntryId = %(int64 msgData.entryId)
                    Type = Cumulative (%msgData.BatchIndex, BatchMessageAcker.NullAcker)
                    Partition = msgData.Partition
                    TopicName = %""
                }
            else
                {
                    LedgerId = %(int64 msgData.ledgerId)
                    EntryId = %(int64 msgData.entryId)
                    Type = Individual
                    Partition = msgData.Partition
                    TopicName = %""
                }
        static member FromByteArrayWithTopic (data: byte[], topicName: string) =
            let initial = MessageId.FromByteArray(data)
            { initial with TopicName = TopicName(topicName).CompleteTopicName }


type internal SendReceipt =
    {
        SequenceId: SequenceId
        LedgerId: LedgerId
        EntryId: EntryId
    }

type internal LogicalAddress = LogicalAddress of DnsEndPoint
type internal PhysicalAddress = PhysicalAddress of DnsEndPoint

type internal Broker =
    {
        LogicalAddress: LogicalAddress
        PhysicalAddress: PhysicalAddress
    }

type CompressionType =
    | None = 0
    | LZ4 = 1
    | ZLib = 2
    | ZStd = 3
    | Snappy = 4

type internal Metadata =
    {
        NumMessages: int
        HasNumMessagesInBatch: bool
        CompressionType: CompressionType
        UncompressedMessageSize: int32
        SchemaVersion: SchemaVersion option
        SequenceId: uint64
    }

type MessageKey =
    {
        PartitionKey: PartitionKey
        IsBase64Encoded: bool
    }

type internal RawMessage =
    {
        MessageId: MessageId    
        Metadata: Metadata
        RedeliveryCount: uint32
        Payload: byte[]
        MessageKey: string
        IsKeyBase64Encoded: bool
        CheckSumValid: bool
        Properties: IReadOnlyDictionary<string, string>
    }

type Message<'T> internal (messageId: MessageId, data: byte[], key: PartitionKey, hasBase64EncodedKey: bool,
                  properties: IReadOnlyDictionary<string, string>, schemaVersion: byte[], sequenceId: SequenceId,
                  getValue: unit -> 'T) =
    /// Get the unique message ID associated with this message.
    member this.MessageId = messageId
    /// Get the raw payload of the message.
    member this.Data = data
    /// Get the key of the message.
    member this.Key = key
    /// Check whether the key has been base64 encoded.
    member this.HasBase64EncodedKey = hasBase64EncodedKey
    /// Return the properties attached to the message.
    member this.Properties = properties
    /// Schema version of the message if the message is produced with schema otherwise null.
    member this.SchemaVersion = schemaVersion
    /// Get the sequence id associated with this message
    member this.SequenceId = sequenceId
    /// Get the de-serialized value of the message, according the configured Schema.
    member this.GetValue() =
        getValue()
    member internal this.WithMessageId messageId =
        Message(messageId, data, key, hasBase64EncodedKey, properties, schemaVersion, sequenceId, getValue)
    /// Get a new instance of the message with updated data
    member this.WithData data =
        Message(messageId, data, key, hasBase64EncodedKey, properties, schemaVersion, sequenceId, getValue)
    /// Get a new instance of the message with updated key
    member this.WithKey (key, hasBase64EncodedKey) =
        Message(messageId, data, key, hasBase64EncodedKey, properties, schemaVersion, sequenceId, getValue)
    /// Get a new instance of the message with updated properties
    member this.WithProperties properties =
        Message(messageId, data, key, hasBase64EncodedKey, properties, schemaVersion, sequenceId, getValue)

type Messages<'T> internal(maxNumberOfMessages: int, maxSizeOfMessages: int64) =

    let mutable currentNumberOfMessages = 0
    let mutable currentSizeOfMessages = 0L

    let messageList = if maxNumberOfMessages > 0 then ResizeArray<Message<'T>>(maxNumberOfMessages) else ResizeArray<Message<'T>>()
    
    member this.Count with get() =
        currentNumberOfMessages
    member this.Size with get() =
        currentSizeOfMessages

    member internal this.IsFull with get() =
        currentNumberOfMessages = maxNumberOfMessages
        || currentSizeOfMessages = maxSizeOfMessages
    
    member internal this.CanAdd(message: Message<'T>) =
        if (maxNumberOfMessages <= 0 && maxSizeOfMessages <= 0L) then
            true
        else
            (maxNumberOfMessages > 0 && currentNumberOfMessages + 1 <= maxNumberOfMessages)
                || (maxSizeOfMessages > 0L && currentSizeOfMessages + (int64 message.Data.Length) <= maxSizeOfMessages)

    member internal this.Add(message: Message<'T>) =
        currentNumberOfMessages <- currentNumberOfMessages + 1
        currentSizeOfMessages <- currentSizeOfMessages + (int64 message.Data.Length)
        messageList.Add(message)

    interface IEnumerable<Message<'T>> with
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> IEnumerator<Message<'T>>

/// <summary>
///     Message builder that constructs a message to be published through a producer.
/// </summary>
type MessageBuilder<'T> =
    val Value: 'T
    val Payload: byte[]
    val Key: MessageKey option
    val Properties: IReadOnlyDictionary<string, string>
    val DeliverAt: Nullable<int64>
    val SequenceId: Nullable<SequenceId>

    internal new (value : 'T, payload: byte[], key : MessageKey option,
            [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string, string>)>] properties : IReadOnlyDictionary<string, string>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<int64>)>] deliverAt : Nullable<int64>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<SequenceId>)>] sequenceId : Nullable<SequenceId>) =
            {
                Value = value
                Key = key
                Properties = if isNull properties then EmptyProps else properties
                DeliverAt = deliverAt
                Payload = payload
                SequenceId = sequenceId
            }
        
        
type internal WriterStream = Stream
type internal Payload = WriterStream -> Task
type internal Connection =
    {
        Input: PipeReader
        Output: WriterStream
        Dispose: unit -> unit
    }
type internal RedeliverSet = HashSet<MessageId>

type internal SingleCallback<'T> = MessageBuilder<'T> * TaskCompletionSource<MessageId>
type internal BatchCallback<'T> = BatchDetails * MessageBuilder<'T> * TaskCompletionSource<MessageId>
type internal PendingCallback<'T> = 
    | SingleCallback of SingleCallback<'T>
    | BatchCallbacks of BatchCallback<'T>[]

type internal PendingMessage<'T> =
    {
        CreatedAt: DateTime
        SequenceId: SequenceId
        Payload: Payload
        Callback : PendingCallback<'T>
    }

type internal BatchItem<'T> =
    {
        Message: MessageBuilder<'T>
        Tcs : TaskCompletionSource<MessageId>
    }

type internal PendingBatch =
    {
        SequenceId: SequenceId
        CompletionSources : TaskCompletionSource<MessageId> list
    }

type internal PulsarResponseType =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult
    | ProducerSuccess of ProducerSuccess
    | TopicsOfNamespace of TopicsOfNamespace
    | LastMessageId of MessageId
    | TopicSchema of TopicSchema option
    | PulsarError
    | Empty

    static member GetPartitionedTopicMetadata = function
        | PartitionedTopicMetadata x -> x
        | _ -> failwith "Incorrect return type"

    static member GetLookupTopicResult = function
        | LookupTopicResult x -> x
        | _ -> failwith "Incorrect return type"

    static member GetProducerSuccess = function
        | ProducerSuccess x -> x
        | _ -> failwith "Incorrect return type"

    static member GetTopicsOfNamespace = function
        | TopicsOfNamespace x -> x
        | _ -> failwith "Incorrect return type"

    static member GetLastMessageId = function
        | LastMessageId msgId -> msgId
        | _ -> failwith "Incorrect return type"
        
    static member GetTopicSchema = function
        | TopicSchema x -> x
        | _ -> failwith "Incorrect return type"

    static member GetEmpty = function
        | Empty -> ()
        | _ -> failwith "Incorrect return type"

type internal ResultOrException<'T> = Result<'T, exn>

type internal SeekData =
    | MessageId of MessageId
    | Timestamp of uint64

type AuthData =
    {
        Bytes: byte[]
    }
    static member INIT_AUTH_DATA = Encoding.UTF8.GetBytes("PulsarAuthInit")

type MessageRoutingMode =
    | SinglePartition = 0
    | RoundRobinPartition = 1
    | CustomPartition = 2

type HashingScheme =
    | DotnetStringHash = 0
    | Murmur3_32Hash = 1


