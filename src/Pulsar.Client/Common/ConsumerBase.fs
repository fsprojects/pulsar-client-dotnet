module Pulsar.Client.Common.ConsumerBase

open System.Collections.Generic
open System.Threading
open Pulsar.Client.Api

type Waiter<'T> =
    CancellationTokenRegistration * AsyncReplyChannel<ResultOrException<Message<'T>>> 
type BatchWaiter<'T> =
    CancellationTokenSource * CancellationTokenRegistration * AsyncReplyChannel<ResultOrException<Messages<'T>>>


let hasEnoughMessagesForBatchReceive (batchReceivePolicy: BatchReceivePolicy) incomingMessagesCount incomingMessagesSize =
    if (batchReceivePolicy.MaxNumMessages <= 0 && batchReceivePolicy.MaxNumBytes <= 0L) then
        false
    else
        (batchReceivePolicy.MaxNumMessages > 0 && incomingMessagesCount >= batchReceivePolicy.MaxNumMessages)
            || (batchReceivePolicy.MaxNumBytes > 0L && incomingMessagesSize >= batchReceivePolicy.MaxNumBytes)
            
let dequeueWaiter (waiters: LinkedList<Waiter<'T>>) =
    let (ctr, ch) = waiters.First.Value
    waiters.RemoveFirst()
    ctr.Dispose()
    ch
        
let dequeueBatchWaiter (batchWaiters: LinkedList<BatchWaiter<'T>>) =
    let (cts, ctr, ch) = batchWaiters.First.Value
    batchWaiters.RemoveFirst()
    ctr.Dispose()
    (cts, ch)