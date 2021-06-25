module internal Pulsar.Client.Internal.Generators

open System.Threading
open Pulsar.Client.Common
open FSharp.UMX
open System
open Microsoft.Extensions.Logging

let mutable private requestId = -1L
let mutable private producerId = -1L
let mutable private consumerId = -1L
let mutable private clientCnxId = -1L

let getNextRequestId(): RequestId =
    let result = % (uint64 <| Interlocked.Increment(&requestId))
    Log.Logger.LogDebug("Next requestId: ", result)
    result

let getNextProducerId(): ProducerId =
    let result = % (uint64 <| Interlocked.Increment(&producerId))
    Log.Logger.LogDebug("Next producerId: ", result)
    result

let getNextConsumerId(): ConsumerId =
    let result = % (uint64 <| Interlocked.Increment(&consumerId))
    Log.Logger.LogDebug("Next consumerId: ", result)
    result

let getNextClientCnxId(): ClientCnxId =
    let result = % (uint64 <| Interlocked.Increment(&clientCnxId))
    Log.Logger.LogDebug("Next clientCnxId: ", result)
    result
    
let getRandomName(): string =
    Guid.NewGuid().ToString().Substring(0, 5)