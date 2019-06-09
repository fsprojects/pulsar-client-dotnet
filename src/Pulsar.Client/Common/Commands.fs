module Pulsar.Client.Common.Commands

open pulsar.proto

let newPartitionMetadataRequest (topicName: string) (requestId: RequestId) : byte[] =
    [||]

let newSend (producerId: ProducerId) (sequenceId: SequenceId) (numMessages: int) (checksumType: ChecksumType) (msgMetadata: MessageMetadata) payload : byte[] =
    [||]