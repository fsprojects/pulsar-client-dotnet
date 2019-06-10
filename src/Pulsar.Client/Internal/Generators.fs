module Pulsar.Client.Internal.Generators

open System.Threading
open Pulsar.Client.Common
open FSharp.UMX

let mutable private requestId = 0L
let mutable private producerId = 0L
let mutable private sequenceId = 0L

let getNextRequestId(): RequestId =
    %Interlocked.Increment(&requestId)

let getNextProducerId(): ProducerId =
    %Interlocked.Increment(&producerId)

let getNextSequenceId(): SequenceId =
    % (uint64 <| Interlocked.Increment(&sequenceId))