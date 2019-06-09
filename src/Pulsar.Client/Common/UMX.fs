[<AutoOpen>]
module internal Pulsar.Client.Common.UMX

[<Measure>] type private ledgerId
[<Measure>] type private entryId
[<Measure>] type private requestId
[<Measure>] type private producerId
[<Measure>] type private sequenceId

type LedgerId = int64<ledgerId>
type EntryId = int64<entryId>
type RequestId = int64<requestId>
type ProducerId = int64<requestId>
type SequenceId = int64<sequenceId>
