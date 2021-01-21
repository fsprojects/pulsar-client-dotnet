[<AutoOpen>]
module internal Pulsar.Client.Common.UMX

open FSharp.UMX

[<Measure>] type private ledgerId
[<Measure>] type private entryId
[<Measure>] type private requestId
[<Measure>] type private producerId
[<Measure>] type private consumerId
[<Measure>] type private transactionCoordinatorId
[<Measure>] type private sequenceId
[<Measure>] type private chunkId
[<Measure>] type private uuid
[<Measure>] type private clientCnxId
[<Measure>] type private completeTopicName
[<Measure>] type private subscription
[<Measure>] type private partitionKey
[<Measure>] type private batchIndex
[<Measure>] type private priorityLevel
[<Measure>] type private timestamp

type LedgerId = int64<ledgerId>
type EntryId = int64<entryId>
type RequestId = uint64<requestId>
type ProducerId = uint64<producerId>
type ConsumerId = uint64<consumerId>
type TransactionCoordinatorId = uint64<transactionCoordinatorId>
type SequenceId = int64<sequenceId>
type ChunkId = int<chunkId>
type Uuid = string<uuid>
type ClientCnxId = uint64<clientCnxId>
type CompleteTopicName = string<completeTopicName>
type SubscriptionName = string<subscription>
type PartitionKey = string<partitionKey>
type BatchIndex = int<batchIndex>
type PriorityLevel = int<priorityLevel>
type TimeStamp = int64<timestamp>