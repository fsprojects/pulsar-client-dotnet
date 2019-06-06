module Pulsar.Client.Internal.Generators

open System.Threading
open FSharp.UMX

let mutable private requestId = 0L

let getNextRequestId() =
    %Interlocked.Increment(&requestId)
