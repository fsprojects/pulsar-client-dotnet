[<AutoOpen>]
module internal Pulsar.Client.Common.UMX

open FSharp.UMX

[<Measure>] type private ledgerId
[<Measure>] type private entryId
[<Measure>] type private requestId
[<Measure>] type private producerId
[<Measure>] type private sequenceId
[<Measure>] type private completeTopicName

type LedgerId = uint64<ledgerId>
type EntryId = uint64<entryId>
type RequestId = int64<requestId>
type ProducerId = int64<producerId>
type SequenceId = uint64<sequenceId>
type CompleteTopicName = string<completeTopicName>
