namespace Pulsar.Client.Common

open System.Net
open System
open pulsar.proto
open System.IO.Pipelines
open Pipelines.Sockets.Unofficial
open System.IO
open System.Threading.Tasks

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
    
type WriterStream = Stream
type SerializedPayload = WriterStream -> Task
type Connection = SocketConnection * WriterStream

type ConnectionState =
    | NotConnected
    | Connected of Connection    

type ProducerMessage = 
    | Connect of (Broker*MailboxProcessor<ProducerMessage>) * AsyncReplyChannel<unit>
    | Reconnect
    | Disconnected of Connection*MailboxProcessor<ProducerMessage>
    | SendReceipt of CommandSendReceipt
    | SendMessage of SerializedPayload * AsyncReplyChannel<unit>

type ConsumerMessage =
    | Connect of (Broker*MailboxProcessor<ConsumerMessage>) * AsyncReplyChannel<unit>
    | Reconnect
    | Disconnected of Connection*MailboxProcessor<ConsumerMessage>
    | AddMessage of Message
    | GetMessage of AsyncReplyChannel<Message>
    | Ack of SerializedPayload * AsyncReplyChannel<unit>