namespace Pulsar.Client.Common

type ChecksumType =
    | Crc32c
    | None

[<CLIMutable>]
type PartitionedTopicMetadata =
    {
        Partitions: int
    }
