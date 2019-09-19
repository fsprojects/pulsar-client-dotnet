namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Threading
open Pulsar.Client.Api

type SinglePartitionMessageRouterImpl (partitionIndex: int) =
    interface IMessageRouter with
        member this.ChoosePartition (numPartitions: int) =
            partitionIndex

type RoundRobinPartitionMessageRouterImpl (startPartitionIndex: int, isBatchingEnabled: bool, maxBatchingDelayMs: int) =
    let mutable partitionIndex = startPartitionIndex
    let maxBatchingDelayMs = Math.Max(1, maxBatchingDelayMs)

    interface IMessageRouter with
        member this.ChoosePartition (numPartitions: int) =
            if isBatchingEnabled
            then
                let currentMs = DateTime.Now.Millisecond
                signSafeMod (currentMs / maxBatchingDelayMs + startPartitionIndex) numPartitions
            else
                signSafeMod (Interlocked.Increment(&partitionIndex)) numPartitions
