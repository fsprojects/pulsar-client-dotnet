module Pulsar.Client.Internal.Generators

open System.Threading
open Pulsar.Client.Common
open FSharp.UMX

let mutable private requestId = 0L

let getNextRequestId(): RequestId =
    %Interlocked.Increment(&requestId)
