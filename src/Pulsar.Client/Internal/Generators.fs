module internal Pulsar.Client.Internal.Generators

open System.Threading
open Pulsar.Client.Common
open FSharp.UMX

let mutable private requestId = 0L
let mutable private producerId = 0L
let mutable private consumerId = 0L
let mutable private sequenceId = 0L


let mutable private clientCnxId = 0

let getNextRequestId(): RequestId =
    % (uint64 <| Interlocked.Increment(&requestId))

let getNextProducerId(): ProducerId =
    % (uint64 <| Interlocked.Increment(&producerId))

let getNextConsumerId(): ConsumerId =
    % (uint64 <| Interlocked.Increment(&consumerId))

let getNextSequenceId(): SequenceId =
    % (uint64 <| Interlocked.Increment(&sequenceId))

let getNextClientCnxId(): int =
    Interlocked.Increment(&clientCnxId)