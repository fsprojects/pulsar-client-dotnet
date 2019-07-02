[<AutoOpen>]
module Pulsar.Client.Common.Tools

open System.Net
open System
open Microsoft.IO

let internal MemoryStreamManager = RecyclableMemoryStreamManager()
let MagicNumber = int16 0x0e01

// Converts

let inline int32ToBigEndian(num : Int32) =
    IPAddress.HostToNetworkOrder(num)

let inline int32FromBigEndian(num : Int32) =
    IPAddress.NetworkToHostOrder(num)

let inline int16FromBigEndian(num : Int16) =
    IPAddress.NetworkToHostOrder(num)

// Exception helper

let throwIf predicate createException arg =
    if predicate(arg) 
    then raise(createException())
    else arg

let invalidArgIf predicate message =
    throwIf predicate (fun() -> ArgumentException(message))

let invalidArgIfBlankString =
    invalidArgIf (String.IsNullOrWhiteSpace)

let throwIfBlankString createException =
    throwIf (String.IsNullOrWhiteSpace) createException

