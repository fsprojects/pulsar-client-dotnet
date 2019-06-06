[<AutoOpen>]
module internal Pulsar.Client.Common.UMX

[<Measure>] type private ledgerId
[<Measure>] type private entryId
[<Measure>] type private requestId

type LedgerId = int64<ledgerId>
type EntryId = int64<entryId>
type RequestId = int64<requestId>
