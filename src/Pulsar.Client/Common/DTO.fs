namespace Pulsar.Client.Common

open System.Net
open System
open pulsar.proto
open System.IO.Pipelines
open Pipelines.Sockets.Unofficial
open System.IO
open System.Threading.Tasks
open FSharp.UMX

type ChecksumType =
    | Crc32c
    | No

type PartitionedTopicMetadata =
    {
        Partitions: uint32
    }

type ProducerSuccess =
    {
        GeneratedProducerName: string
    }

type LookupTopicResult =
    {
        Proxy: bool
        BrokerServiceUrl : string
    }

type TopicsOfNamespace =
    {
        Topics : string list
    }

type SubscriptionType =
    | Exclusive = 0
    | Shared = 1
    | Failover = 2

type TopicDomain =
    | Persistent
    | NonPersistent

type MessageId =
    {
        LedgerId: LedgerId
        EntryId: EntryId
        Partition: int
    }
    with
        static member FromMessageIdData(messageIdData: MessageIdData) =
            {
                LedgerId = %messageIdData.ledgerId
                EntryId = %messageIdData.entryId
                Partition = messageIdData.Partition
            }
        member this.ToMessageIdData() =
            MessageIdData(
                ledgerId = %this.LedgerId,
                entryId = %this.EntryId,
                Partition = this.Partition
            )

type LogicalAddress = LogicalAddress of DnsEndPoint
type PhysicalAddress = PhysicalAddress of DnsEndPoint

type Broker =
    {
        LogicalAddress: LogicalAddress
        PhysicalAddress: PhysicalAddress
    }

type Message =
    {
        MessageId: MessageId
        Payload: byte[]
    }

type WriterStream = Stream
type Payload = WriterStream -> Task
type Connection = SocketConnection * WriterStream

type PendingMessage =
    {
        SequenceId: SequenceId
        Payload: Payload
        Tcs : TaskCompletionSource<MessageId>
    }

type PulsarTypes =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult
    | ProducerSuccess of ProducerSuccess
    | TopicsOfNamespace of TopicsOfNamespace
    | Error
    | Empty

type ProducerMessage =
    | ConnectionOpened
    | ConnectionClosed
    | SendReceipt of CommandSendReceipt
    | BeginSendMessage of byte[] * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendMessage of PendingMessage
    | SendError of CommandSendError
    | Close of AsyncReplyChannel<Task>

type ConsumerMessage =
    | ConnectionOpened
    | ConnectionClosed
    | ReachedEndOfTheTopic
    | MessageRecieved of Message
    | GetMessage of AsyncReplyChannel<Message>
    | Ack of Payload * AsyncReplyChannel<unit>
    | SendAndForget of Payload
    | Close of AsyncReplyChannel<Task>
    | Unsubscribe of AsyncReplyChannel<Task>