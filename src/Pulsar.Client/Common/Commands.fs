module Pulsar.Client.Common.Commands

open pulsar.proto

type ChecksumType =
    | Crc32c
    | None

let newPartitionMetadataRequest (topicName: string) (requestId: RequestId) : byte[] =
    [||]

let newSend producerId sequenceId numMessages (checksumType: ChecksumType) (msgMetadata: MessageMetadata) payload : byte[] =
    [||]