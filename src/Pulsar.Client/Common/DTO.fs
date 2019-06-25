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

type LookupTopicResult = {
    BrokerServiceUrl : string
}

type MessageId =
    {
        LedgerId: LedgerId
        EntryId: EntryId
        Partition: int
    } 
    with static member FromMessageIdData(messageIdData: MessageIdData) =
            {
                LedgerId = %messageIdData.ledgerId
                EntryId = %messageIdData.entryId
                Partition = messageIdData.Partition
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
        MessageId: MessageId
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