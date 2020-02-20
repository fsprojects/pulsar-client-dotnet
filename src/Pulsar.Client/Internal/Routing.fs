namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Threading
open Pulsar.Client.Api
open FSharp.UMX

type internal SinglePartitionMessageRouterImpl (partitionIndex: int, hashFun: string -> int) =
    interface IMessageRouter with
        member this.ChoosePartition (messageKey, numPartitions) =
            if String.IsNullOrEmpty(%messageKey) then
                partitionIndex
            else
                // If the message has a key, it supersedes the single partition routing policy
                signSafeMod (hashFun %messageKey) numPartitions

type internal RoundRobinPartitionMessageRouterImpl (startPartitionIndex: int, isBatchingEnabled: bool, partitionSwitchMs: int, hashFun: string -> int) =
    let mutable partitionIndex = startPartitionIndex
    let partitionSwitchMs = Math.Max(1, partitionSwitchMs)

    interface IMessageRouter with
        member this.ChoosePartition (messageKey, numPartitions) =
            if String.IsNullOrEmpty(%messageKey) then
                if isBatchingEnabled
                then
                    let currentMs = DateTime.Now.Millisecond
                    signSafeMod (currentMs / partitionSwitchMs + startPartitionIndex) numPartitions
                else
                    signSafeMod (Interlocked.Increment(&partitionIndex)) numPartitions
            else
                // If the message has a key, it supersedes the single partition routing policy
                signSafeMod (hashFun %messageKey) numPartitions
