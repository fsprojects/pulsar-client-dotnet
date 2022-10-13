namespace Pulsar.Client.Transaction

open System
open System.Collections.Generic
open System.Threading

open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Api
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
        Commit: TxnId -> Task<unit>
        Abort: TxnId -> Task<unit>
    }

type ConsumerTxnOperations =
    {
        ClearIncomingMessagesAndGetMessageNumber: unit -> Async<int>
        IncreaseAvailablePermits: int -> unit
    }

type State =
    | OPEN
    | COMMITTING
    | ABORTING
    | COMMITTED
    | ABORTED
    | ERROR

[<AllowNullLiteral>]
type Transaction internal (timeout: TimeSpan, txnOperations: TxnOperations, txnId: TxnId) as this =

    let mutable state = OPEN
    let registerPartitionMap = Dictionary<CompleteTopicName, Task<unit>>()
    let registerSubscriptionMap = Dictionary<CompleteTopicName * SubscriptionName, Task<unit>>()
    let sendTasks = ResizeArray<Task<MessageId>>()
    let ackTasks = ResizeArray<Task<Unit>>()
    let cumulativeAckConsumers = Dictionary<ConsumerId, ConsumerTxnOperations>()
    let lockObj = Object()

    let allOpComplete() =
        seq {
            for sendTask in sendTasks do
                yield (sendTask :> Task)
            for ackTask in ackTasks do
                yield (ackTask :> Task)
        } |> Task.WhenAll


    let checkIfOpen f =
        if this.State = OPEN then
            f()
        else
            raise <| InvalidTxnStatusException $"{txnId} with unexpected state {state}, expected OPEN state!"

    let executeInsideLockIfOpen f =
        checkIfOpen (fun () ->
            lock lockObj (fun () ->
                checkIfOpen f
        ))

    do asyncDelayTask timeout (fun () ->
        match this.State with
        | OPEN ->
            this.Abort()
        | _ ->
            unitTask
    )

    member this.State
        with get() = Volatile.Read(&state)
        and private set value = Volatile.Write(&state, value)

    member internal this.RegisterProducedTopic(topic: CompleteTopicName) =
        executeInsideLockIfOpen (fun () ->
            match registerPartitionMap.TryGetValue topic with
            | true, t -> t
            | false, _ ->
                let t = txnOperations.AddPublishPartitionToTxn(txnId, topic)
                registerPartitionMap.Add(topic, t)
                t
        )

    member internal this.RegisterSendOp(sendTask: Task<MessageId>) =
        lock lockObj (fun () ->
            sendTasks.Add(sendTask)
        )

    member internal this.RegisterAckedTopic(topic: CompleteTopicName, subscription: SubscriptionName) =
        executeInsideLockIfOpen (fun () ->
            let key = topic, subscription
            match registerSubscriptionMap.TryGetValue key with
            | true, t -> t
            | false, _ ->
                let t = txnOperations.AddSubscriptionToTxn(txnId, topic, subscription)
                registerSubscriptionMap.Add(key, t)
                t
        )

    member internal this.RegisterAckOp(ackTask: Task<Unit>) =
        lock lockObj (fun () ->
            ackTasks.Add(ackTask)
        )

    member internal this.RegisterCumulativeAckConsumer(consumerId: ConsumerId, consumerOperations: ConsumerTxnOperations) =
        lock lockObj (fun () ->
            cumulativeAckConsumers.[consumerId] <- consumerOperations
        )

    member this.Id = txnId

    member private this.CommitInner() =
        let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
        this.State <- COMMITTING
        backgroundTask {
            try
                do! allOpComplete()
            with Flatten ex ->
                do! this.AbortInner()
                tcs.SetException ex
            try
                do! txnOperations.Commit(txnId)
                this.State <- COMMITTED
                tcs.SetResult ()
            with Flatten ex ->
                if (ex :? TransactionNotFoundException) || (ex :? InvalidTxnStatusException) then
                    this.State <- ERROR
                tcs.SetException ex
            return! tcs.Task
        }

    member private this.AbortInner() =
        let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
        this.State <- ABORTING
        backgroundTask {
            try
                do! allOpComplete()
            with ex ->
                Log.Logger.LogError(ex, "Error during abort txnId={0}", txnId)
            let! cumulativeConsumersData =
                cumulativeAckConsumers
                |> Seq.map(fun (KeyValue(_, consumer)) ->
                    backgroundTask {
                        let! permits = consumer.ClearIncomingMessagesAndGetMessageNumber()
                        return (consumer, permits)
                    })
                |> Task.WhenAll
            try
                try
                    do! txnOperations.Abort(txnId)
                    this.State <- ABORTED
                    tcs.SetResult ()
                with Flatten ex ->
                    if (ex :? TransactionNotFoundException) || (ex :? InvalidTxnStatusException) then
                        this.State <- ERROR
                    tcs.SetException ex
            finally
                for consumer, permits in cumulativeConsumersData do
                    consumer.IncreaseAvailablePermits permits
                cumulativeAckConsumers.Clear()
        }

    member this.Commit() : Task<unit> =
        checkIfOpen (fun () ->
            this.CommitInner()
        )


    member this.Abort(): Task<unit> =
        checkIfOpen (fun () ->
            this.AbortInner()
        )

    override this.ToString() =
        $"Txn({txnId})"