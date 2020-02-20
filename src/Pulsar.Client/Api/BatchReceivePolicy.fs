namespace Pulsar.Client.Api

open System

type BatchReceivePolicy(maxNumMessages : int,
                        maxNumBytes : int64,
                        timeout : TimeSpan) =

    /// Default policy: -1 messages, 10 Mb, 100 ms (negative means no limit)
    new() = BatchReceivePolicy(-1)

    new(maxNumMessages) = BatchReceivePolicy(maxNumMessages, 10485760L)

    new(maxNumMessages, maxNumBytes) = BatchReceivePolicy(maxNumMessages, maxNumBytes, TimeSpan.FromMilliseconds(100.0))

    member this.Verify () =
        if (maxNumMessages <= 0 && maxNumBytes <= 0L && timeout = TimeSpan.Zero) then
            failwith "At least one of maxNumMessages, maxNumBytes, timeout must be specified."
            
    member this.MaxNumMessages = maxNumMessages
    member this.MaxNumBytes = maxNumBytes
    member this.Timeout = timeout