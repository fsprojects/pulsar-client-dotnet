[<AutoOpen>]
module Pulsar.Client.Common.Tools

open System.Net
open System
open Microsoft.IO
open System.Runtime.ExceptionServices

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

let invalidArgIfNotGreaterThanZero =
    invalidArgIf ((>=) 0)

let throwIfBlankString createException =
    throwIf (String.IsNullOrWhiteSpace) createException

let throwIfDefault createException (value: 'a) =
    throwIf (fun (arg) -> arg = Unchecked.defaultof<'a>) createException value

let reraize ex =
    (ExceptionDispatchInfo.Capture ex).Throw()