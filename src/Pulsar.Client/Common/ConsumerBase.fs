module Pulsar.Client.Common.ConsumerBase

open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Internal
open System

type UserCancellation = CancellationTokenRegistration option
type BatchCancellation = CancellationTokenSource

type Waiter<'T> =
    UserCancellation * TaskCompletionSource<Message<'T>>
    
type BatchWaiter<'T> =
    BatchCancellation * UserCancellation * TaskCompletionSource<Messages<'T>>

type ReceiveCallback<'T> = {
    CancellationToken: CancellationToken
    MessageChannel: TaskCompletionSource<Message<'T>>
}

type ReceiveCallbacks<'T> = {
    CancellationToken: CancellationToken
    MessagesChannel: TaskCompletionSource<Messages<'T>>
}

let hasEnoughMessagesForBatchReceive (batchReceivePolicy: BatchReceivePolicy) incomingMessagesCount incomingMessagesSize =
    if (batchReceivePolicy.MaxNumMessages <= 0 && batchReceivePolicy.MaxNumBytes <= 0L) then
        false
    else
        (batchReceivePolicy.MaxNumMessages > 0 && incomingMessagesCount >= batchReceivePolicy.MaxNumMessages)
            || (batchReceivePolicy.MaxNumBytes > 0L && incomingMessagesSize >= batchReceivePolicy.MaxNumBytes)
            
let dequeueWaiter (waiters: LinkedList<Waiter<'T>>) =
    let (ctrOpt, ch) = waiters.First.Value
    waiters.RemoveFirst()
    ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
    ch
        
let dequeueBatchWaiter (batchWaiters: LinkedList<BatchWaiter<'T>>) =
    let (cts, ctrOpt, ch) = batchWaiters.First.Value
    batchWaiters.RemoveFirst()
    ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
    cts.Cancel()
    cts.Dispose()
    ch
    
let getConsumerName configName =
    if String.IsNullOrEmpty configName then
        Generators.getRandomName()
    else
        configName