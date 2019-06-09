namespace Pulsar.Client.Common

open System.Net

type ChecksumType =
    | Crc32c
    | None

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
