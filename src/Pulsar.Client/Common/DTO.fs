namespace Pulsar.Client.Common

open System.Net
open System
open pulsar.proto
open System.IO.Pipelines
open Pipelines.Sockets.Unofficial
open System.IO
open System.Threading.Tasks
open FSharp.UMX
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging
open System.Collections.Generic

type ChecksumType =
    | Crc32c
    | No

type PartitionedTopicMetadata =
    {
        Partitions: int
    }

type ProducerSuccess =
    {
        GeneratedProducerName: string
    }

type LookupTopicResult =
    {
        Proxy: bool
        BrokerServiceUrl : string
        Redirect: bool
        Authoritative: bool
    }

type TopicsOfNamespace =
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

type TopicDomain =
    | Persistent
    | NonPersistent

type SubscriptionMode =
    | Durable
    | NonDurable

type AckType =
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
        member this.ToMessageIdData() =
            MessageIdData(
                ledgerId = uint64 %this.LedgerId,
                entryId = uint64 %this.EntryId,
                Partition = this.Partition
            )
        member this.PrevBatchMessageId
            with get() =
                {
                    this with EntryId = this.EntryId - %1L
                }

type SendReceipt =
    {
        SequenceId: SequenceId
        LedgerId: LedgerId
        EntryId: EntryId
    }

type LogicalAddress = LogicalAddress of DnsEndPoint
type PhysicalAddress = PhysicalAddress of DnsEndPoint

type Broker =
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

type Metadata =
    {
        NumMessages: int
        HasNumMessagesInBatch: bool
        CompressionType: CompressionType
        UncompressedMessageSize: int32
    }
    with
        static member FromMessageMetadata(messageMetadata: MessageMetadata) =

            let mapCompressionType = function
                | pulsar.proto.CompressionType.None -> CompressionType.None
                | pulsar.proto.CompressionType.Lz4 -> CompressionType.LZ4
                | pulsar.proto.CompressionType.Zlib -> CompressionType.ZLib
                | pulsar.proto.CompressionType.Zstd -> CompressionType.ZStd
                | pulsar.proto.CompressionType.Snappy -> CompressionType.Snappy
                | _ -> CompressionType.None

            {
                NumMessages = messageMetadata.NumMessagesInBatch
                HasNumMessagesInBatch = messageMetadata.ShouldSerializeNumMessagesInBatch()
                CompressionType = messageMetadata.Compression |> mapCompressionType
                UncompressedMessageSize = messageMetadata.UncompressedSize |> int32
            }

type Message =
    {
        MessageId: MessageId
        Metadata: Metadata
        RedeliveryCount: uint32
        Payload: byte[]
    }

type WriterStream = Stream
type Payload = WriterStream -> Task
type Connection = SocketConnection * WriterStream
type TrackerState = HashSet<MessageId>

type PendingCallback =
    | SingleCallback of TaskCompletionSource<MessageId>
    | BatchCallbacks of TaskCompletionSource<MessageId>[]

type PendingMessage =
    {
        CreatedAt: DateTime
        SequenceId: SequenceId
        Payload: Payload
        Callback : PendingCallback
    }

type BatchItem =
    {
        Data: byte[]
        Tcs : TaskCompletionSource<MessageId>
    }

type PendingBatch =
    {
        SequenceId: SequenceId
        CompletionSources : TaskCompletionSource<MessageId> list
    }

type PulsarResponseType =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult
    | ProducerSuccess of ProducerSuccess
    | TopicsOfNamespace of TopicsOfNamespace
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

    static member GetEmpty req =
        match req with
        | Empty -> ()
        | _ -> failwith "Incorrect return type"

type MessageOrException =
    | Message of Message
    | Exn of exn

type ProducerMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of obj // ClientCnx
    | SendReceipt of SendReceipt
    | BeginSendMessage of byte[] * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendMessage of PendingMessage
    | RecoverChecksumError of SequenceId
    | Terminated
    | TimeoutCheck
    | Close of AsyncReplyChannel<Task>
    | StoreBatchItem of byte[] * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendBatchMessage

type ConsumerMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of obj // ClientCnx
    | ReachedEndOfTheTopic
    | MessageReceived of Message
    | Receive of AsyncReplyChannel<MessageOrException>
    | Acknowledge of MessageId * AckType * AsyncReplyChannel<bool>
    | RedeliverUnacknowledged of TrackerState * AsyncReplyChannel<unit>
    | RedeliverAllUnacknowledged of AsyncReplyChannel<unit>
    | SendFlowPermits of int
    | Close of AsyncReplyChannel<Task>
    | Unsubscribe of AsyncReplyChannel<Task>

type MessageRoutingMode =
    | SinglePartition = 0
    | RoundRobinPartition = 1
    | CustomPartition = 2


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

// custom exception
exception ConnectionFailedOnSend of string


