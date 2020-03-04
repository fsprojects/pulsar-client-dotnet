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

type internal ProducerSuccess =
    {
        GeneratedProducerName: string
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
        SequenceId: uint64
    }

type internal RawMessage =
    {
        MessageId: MessageId    
        Metadata: Metadata
        RedeliveryCount: uint32
        Payload: byte[]
        MessageKey: MessageKey
        Properties: IReadOnlyDictionary<string, string>
    }

type Message =
    {
        MessageId: MessageId
        Data: byte[]
        Key: string
        Properties: IReadOnlyDictionary<string, string>
        SequenceId: uint64
    }

type Messages internal(maxNumberOfMessages: int, maxSizeOfMessages: int64) =

    let mutable currentNumberOfMessages = 0
    let mutable currentSizeOfMessages = 0L

    let messageList = if maxNumberOfMessages > 0 then ResizeArray<Message>(maxNumberOfMessages) else ResizeArray<Message>()

    
    member this.Count with get() =
        currentNumberOfMessages
    member this.Size with get() =
        currentSizeOfMessages

    member internal this.IsFull with get() =
        currentNumberOfMessages = maxNumberOfMessages
        || currentSizeOfMessages = maxSizeOfMessages
    
    member internal this.CanAdd(message: Message) =
        if (maxNumberOfMessages <= 0 && maxSizeOfMessages <= 0L) then
            true
        else
            (maxNumberOfMessages > 0 && currentNumberOfMessages + 1 <= maxNumberOfMessages)
                || (maxSizeOfMessages > 0L && currentSizeOfMessages + (int64 message.Data.Length) <= maxSizeOfMessages)

    member internal this.Add(message: Message) =
        currentNumberOfMessages <- currentNumberOfMessages + 1
        currentSizeOfMessages <- currentSizeOfMessages + (int64 message.Data.Length)
        messageList.Add(message)

    interface IEnumerable<Message> with
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> IEnumerator<Message>

/// <summary>
///     Message builder that constructs a message to be published through a producer.
/// </summary>
type MessageBuilder =
    val Value: byte[]
    val Key: MessageKey
    val Properties: IReadOnlyDictionary<string, string>
    val DeliverAt: Nullable<int64>
    val SequenceId: Nullable<uint64>

    /// <summary>
    ///     Constructs <see cref="Pulsar.Client.Common.MessageBuilder" />
    /// </summary>
    /// <param name="value">Message data serialized to array of bytes.</param>
    /// <param name="properties">The readonly dictionary with message properties.</param>
    /// <param name="deliverAt">Unix timestamp in milliseconds after which message should be delivered to consumer(s).</param>
    /// <param name="sequenceId">
    ///     Specify a custom sequence id for the message being published.
    ///     The sequence id can be used for deduplication purposes and it needs to follow these rules:
    ///         - <c>sequenceId >= 0</c>
    ///         - Sequence id for a message needs to be greater than sequence id for earlier messages:
    ///             <c>sequenceId(N+1) > sequenceId(N)</c>
    ///         - It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
    ///             <c>sequenceId</c> could represent an offset or a cumulative size.
    /// </param>
    /// <remarks>
    ///     This <paramref name="deliverAt" /> timestamp must be expressed as unix time milliseconds based on UTC.
    ///     For example: <code>DateTimeOffset.UtcNow.AddSeconds(2.0).ToUnixTimeMilliseconds()</code>.
    /// </remarks>
    new (value : byte[],
            [<Optional; DefaultParameterValue(null:string)>] key : string,
            [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string, string>)>] properties : IReadOnlyDictionary<string, string>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<int64>)>] deliverAt : Nullable<int64>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<uint64>)>] sequenceId : Nullable<uint64>) =
            {
                Value = value
                Key = if isNull key then %"" else %key
                Properties = if isNull properties then EmptyProps else properties
                DeliverAt = deliverAt
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

type internal SingleCallback = MessageBuilder * TaskCompletionSource<MessageId>
type internal BatchCallback = BatchDetails * MessageBuilder * TaskCompletionSource<MessageId>
type internal PendingCallback = 
    | SingleCallback of SingleCallback
    | BatchCallbacks of BatchCallback[]

type internal PendingMessage =
    {
        CreatedAt: DateTime
        SequenceId: SequenceId
        Payload: Payload
        Callback : PendingCallback
    }

type internal BatchItem =
    {
        Message: MessageBuilder
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
    | Error
    | Empty

    static member GetPartitionedTopicMetadata req =
        match req with
        | PartitionedTopicMetadata x -> x
        | _ -> failwith "Incorrect return type"

    static member GetLookupTopicResult req =
        match req with
        | LookupTopicResult x -> x
        | _ -> failwith "Incorrect return type"

    static member GetProducerSuccess req =
        match req with
        | ProducerSuccess x -> x
        | _ -> failwith "Incorrect return type"

    static member GetTopicsOfNamespace req =
        match req with
        | TopicsOfNamespace x -> x
        | _ -> failwith "Incorrect return type"

    static member GetLastMessageId req =
        match req with
        | LastMessageId msgId -> msgId
        | _ -> failwith "Incorrect return type"

    static member GetEmpty req =
        match req with
        | Empty -> ()
        | _ -> failwith "Incorrect return type"

type internal ResultOrException<'T> =
    | Result of 'T
    | Exn of exn

type internal SeekData =
    | MessageId of MessageId
    | Timestamp of uint64

type AuthData =
    {
        Bytes: byte[]
    }
    static member INIT_AUTH_DATA = Encoding.UTF8.GetBytes("PulsarAuthInit")

type internal ProducerMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of obj // ClientCnx
    | AckReceived of SendReceipt
    | BeginSendMessage of MessageBuilder * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendMessage of PendingMessage
    | RecoverChecksumError of SequenceId
    | Terminated
    | Close of AsyncReplyChannel<Task>
    | StoreBatchItem of MessageBuilder * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendBatchTick
    | SendTimeoutTick

type internal ConsumerMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of obj // ClientCnx
    | ReachedEndOfTheTopic
    | MessageReceived of RawMessage
    | Receive of AsyncReplyChannel<ResultOrException<Message>>
    | BatchReceive of AsyncReplyChannel<ResultOrException<Messages>>
    | SendBatchByTimeout
    | Acknowledge of MessageId * AckType
    | NegativeAcknowledge of MessageId
    | RedeliverUnacknowledged of RedeliverSet * AsyncReplyChannel<unit>
    | RedeliverAllUnacknowledged of AsyncReplyChannel<unit>
    | SeekAsync of SeekData * AsyncReplyChannel<ResultOrException<unit>>
    | SendFlowPermits of int
    | HasMessageAvailable of AsyncReplyChannel<Task<bool>>
    | ActiveConsumerChanged of bool
    | Close of AsyncReplyChannel<ResultOrException<unit>>
    | Unsubscribe of AsyncReplyChannel<ResultOrException<unit>>

type MessageRoutingMode =
    | SinglePartition = 0
    | RoundRobinPartition = 1
    | CustomPartition = 2

type HashingScheme =
    | DotnetStringHash = 0
    | Murmur3_32Hash = 1

exception InvalidServiceURL
exception InvalidConfigurationException of string
exception NotFoundException of string
exception TimeoutException of string
exception IncompatibleSchemaException of string
exception LookupException of string
exception TooManyRequestsException of string
exception ConnectException of string
exception AlreadyClosedException of string
exception TopicTerminatedException of string
exception AuthenticationException of string
exception AuthorizationException of string
exception GettingAuthenticationDataException of string
exception UnsupportedAuthenticationException of string
exception BrokerPersistenceException of string
exception BrokerMetadataException of string
exception ProducerBusyException of string
exception ConsumerBusyException of string
exception NotConnectedException of string
exception InvalidMessageException of string
exception InvalidTopicNameException of string
exception NotSupportedException of string
exception ProducerQueueIsFullError of string
exception ProducerBlockedQuotaExceededError of string
exception ProducerBlockedQuotaExceededException of string
exception ChecksumException of string
exception CryptoExceptionof of string
exception TopicDoesNotExistException of string

// custom exception
exception ConnectionFailedOnSend of string
exception MaxMessageSizeChanged of int


