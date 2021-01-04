namespace Pulsar.Client.Transaction

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open Pulsar.Client.Common

type TxnId = {
    MostSigBits: uint64
    LeastSigBits: uint64
}

type TxnOperations =
    {
        AddPublishPartitionToTxn: TxnId * CompleteTopicName -> Task<unit>
        AddSubscriptionToTxn: TxnId * CompleteTopicName * SubscriptionName -> Task<unit>
    }
    
type ConsumerTxnOperations =
    {
        ClearIncomingMessagesAndGetMessageNumber: unit -> unit
        IncreaseAvailablePermits: unit -> unit
    }

[<AllowNullLiteral>]
type Transaction internal (timeout: TimeSpan, txnOperations: TxnOperations, txnId: TxnId) =
    
    let producedTopics = ConcurrentDictionary<CompleteTopicName, unit>()
    let ackedTopics = ConcurrentDictionary<CompleteTopicName, unit>()
    let cumulativeAckConsumers = ConcurrentDictionary<ConsumerId, ConsumerTxnOperations>()
    
    member internal this.RegisterProducedTopic(topic: CompleteTopicName) =
        if producedTopics.TryAdd(topic, ()) then
            // we need to issue the request to TC to register the produced topic
            txnOperations.AddPublishPartitionToTxn(txnId, topic)
        else
            Task.FromResult()
        
    member internal this.RegisterAckedTopic(topic: CompleteTopicName, subscription: SubscriptionName) =
        if ackedTopics.TryAdd(topic, ()) then
            // we need to issue the request to TC to register the acked topic
            txnOperations.AddSubscriptionToTxn(txnId, topic, subscription)
        else
            Task.FromResult()
            
    member internal this.RegisterCumulativeAckConsumer(consumerId: ConsumerId, consumerOperations: ConsumerTxnOperations) =
        cumulativeAckConsumers.TryAdd(consumerId, consumerOperations) |> ignore
    
    member this.Id = txnId
    
    member this.Commit() =
        ()
        
    member this.Abort() =
        ()