namespace Pulsar.Client.Common

open System.Net
open System
open pulsar.proto
open System.IO.Pipelines
open Pipelines.Sockets.Unofficial

type ChecksumType =
    | Crc32c
    | No

[<CLIMutable>]
type PartitionedTopicMetadata =
    {
        Partitions: uint32
    }

type Broker = 
    {
        LogicalAddress: EndPoint
        PhysicalAddress: EndPoint
    }

type SendAck =
    {
         SequenceId: SequenceId
         LedgerId: LedgerId
         EntryId: EntryId
    }

type Message =
    {
        MessageId: MessageIdData
        Payload: byte[]
    }

type ConnectionState =
    | NotConnected
    | Connected of SocketConnection    

type ProducerMessage = 
    | Connect of (Broker*MailboxProcessor<ProducerMessage>) * AsyncReplyChannel<unit>
    | SendReceipt of CommandSendReceipt
    | SendMessage of ReadOnlyMemory<byte> * AsyncReplyChannel<FlushResult>

type ConsumerMessage =
    | Connect of (Broker*MailboxProcessor<ConsumerMessage>) * AsyncReplyChannel<unit>
    | AddMessage of Message
    | GetMessage of AsyncReplyChannel<Message>
    | Ack of ReadOnlyMemory<byte> * AsyncReplyChannel<FlushResult>