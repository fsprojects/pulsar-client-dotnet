module Pulsar.Client.Common.Batch

open Pulsar.Client.Api

let hasEnoughMessagesForBatchReceive (batchReceivePolicy: BatchReceivePolicy) incomingMessagesCount incomingMessagesSize =
    if (batchReceivePolicy.MaxNumMessages <= 0 && batchReceivePolicy.MaxNumBytes <= 0L) then
        false
    else
        (batchReceivePolicy.MaxNumMessages > 0 && incomingMessagesCount >= batchReceivePolicy.MaxNumMessages)
            || (batchReceivePolicy.MaxNumBytes > 0L && incomingMessagesSize >= batchReceivePolicy.MaxNumBytes)