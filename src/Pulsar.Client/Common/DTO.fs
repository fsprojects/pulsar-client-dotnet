namespace Pulsar.Client.Common

open System.Net

type ChecksumType =
    | Crc32c
    | No

[<CLIMutable>]
type PartitionedTopicMetadata =
    {
        Partitions: int
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