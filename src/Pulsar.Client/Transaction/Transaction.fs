namespace Pulsar.Client.Transaction

open System
open System.Collections.Concurrent
open System.Collections.Generic
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging

type TxnId = {
    MostSigBits: uint64
    LeastSigBits: uint64
}
with
    override this.ToString() =
        $"{this.MostSigBits}:{this.LeastSigBits}"

type TxnOperations =
    {
        AddPublishPartitionToTxn: TxnId * CompleteTopicName -> Task<unit>
        AddSubscriptionToTxn: TxnId * CompleteTopicName * SubscriptionName -> Task<unit>
        Commit: TxnId * MessageId seq -> Task<unit>
        Abort: TxnId * MessageId seq -> Task<unit>
    }
    
type ConsumerTxnOperations =
    {
        ClearIncomingMessagesAndGetMessageNumber: unit -> Async<int>
        IncreaseAvailablePermits: int -> unit
    }

[<AllowNullLiteral>]
type Transaction internal (timeout: TimeSpan, txnOperations: TxnOperations, txnId: TxnId) =
    
    let producedTopics = Dictionary<CompleteTopicName, Task<unit>>()
    let ackedTopics = Dictionary<CompleteTopicName, Task<unit>>()
    let sendTasks = ResizeArray<Task<MessageId>>()
    let ackTasks = ResizeArray<Task<Unit>>()
    let cumulativeAckConsumers = Dictionary<ConsumerId, ConsumerTxnOperations>()
    let mutable allowOperations = false
    let lockObj = Object()
    
    let allOpComplete() =
        seq {
            for sendTask in sendTasks do
                yield (sendTask :> Task)
            for ackTask in sendTasks do
                yield (ackTask :> Task)
        } |> Task.WhenAll
        
    let executeInsideLock f errorMsg =
        if allowOperations then
            lock lockObj (fun () ->
                if allowOperations then
                    f()
                else
                    failwith errorMsg
            )
        else
            failwith errorMsg
    
    member internal this.RegisterProducedTopic(topic: CompleteTopicName) =
        executeInsideLock (fun () ->
            // we need to issue the request to TC to register the produced topic
            match producedTopics.TryGetValue topic with
            | true, task ->
                task
            | _ ->
                txnOperations.AddPublishPartitionToTxn(txnId, topic)
        ) "Can't RegisterProducedTopic while transaction is closing"
        
    member internal this.RegisterAckedTopic(topic: CompleteTopicName, subscription: SubscriptionName) =
        executeInsideLock (fun () ->
            // we need to issue the request to TC to register the acked topic
            match ackedTopics.TryGetValue topic with
            | true, task ->
                task
            | _ ->
                txnOperations.AddSubscriptionToTxn(txnId, topic, subscription)
        ) "Can't RegisterProducedTopic while transaction is closing"
            
    member internal this.RegisterCumulativeAckConsumer(consumerId: ConsumerId, consumerOperations: ConsumerTxnOperations) =
        executeInsideLock (fun () ->
            cumulativeAckConsumers.[consumerId] <- consumerOperations
        ) "Can't ack message cumulatively while transaction is closing"
    
    member internal this.RegisterSendOp(sendTask: Task<MessageId>) =
        executeInsideLock (fun () ->
            sendTasks.Add(sendTask)
        ) "Can't send message while transaction is closing"
       
    member internal this.RegisterAckOp(ackTask: Task<Unit>) =
        executeInsideLock (fun () ->
            ackTasks.Add(ackTask)
        ) "Can't ack message while transaction is closing"
    
    member this.Id = txnId
    
    member private this.AbortInner() =
        task {
            try
                do! allOpComplete()
            with ex ->
                Log.Logger.LogError(ex, "Error during abort txnId={0}", txnId)
            let msgIds =
                sendTasks
                |> Seq.where (fun t -> t.IsCompleted)
                |> Seq.map (fun t -> t.Result)
            let! cumulativeConsumersData =
                cumulativeAckConsumers
                |> Seq.map(fun (KeyValue(_, v)) ->
                    async {
                        let! permits = v.ClearIncomingMessagesAndGetMessageNumber()
                        return (permits, v)
                    })
                |> Async.Parallel
            try
                return! txnOperations.Abort(txnId, msgIds)
            finally
                cumulativeConsumersData
                |> Seq.iter(fun (permits, consumer) -> consumer.IncreaseAvailablePermits(permits))
                cumulativeAckConsumers.Clear()
        }
    
    member private this.CommitInner() =
        task {
            try
                do! allOpComplete()
            with ex ->
                do! this.AbortInner()
                reraize ex
            let msgIds =
                sendTasks |> Seq.map (fun t -> t.Result)
            return! txnOperations.Commit(txnId, msgIds)
        }
    
    member this.Commit() =
        executeInsideLock (fun () ->
            allowOperations <- false
            this.CommitInner()
        ) "Can't commit while transaction is closing"
        
    member this.Abort() =
        executeInsideLock (fun () ->
            allowOperations <- false
            this.AbortInner()
        ) "Can't abort while transaction is closing"
        
    override this.ToString() =
        $"Txn({txnId})"