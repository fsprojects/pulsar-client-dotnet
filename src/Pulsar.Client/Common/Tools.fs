[<AutoOpen>]
module Pulsar.Client.Common.Tools

open System.Net
open System
open Microsoft.IO
open System.Runtime.ExceptionServices
open System.Collections.Generic

let internal MemoryStreamManager = RecyclableMemoryStreamManager()
let internal MagicNumber = int16 0x0e01
let internal RandomGenerator = Random()
let internal EmptyProps: IReadOnlyDictionary<string, string> = readOnlyDict []

// Converts

let inline int32ToBigEndian(num : Int32) =
    IPAddress.HostToNetworkOrder(num)

let inline int32FromBigEndian(num : Int32) =
    IPAddress.NetworkToHostOrder(num)

let inline int16FromBigEndian(num : Int16) =
    IPAddress.NetworkToHostOrder(num)

// Exception helper

let throwIf predicate createException arg =
    if predicate(arg) then
        raise(createException())
    else
        arg

let invalidArgIf predicate message =
    throwIf predicate (fun() -> ArgumentException(message))

let invalidArgIfTrue value message =
    if value then raise (ArgumentException(message))

let invalidArgIfBlankString =
    invalidArgIf (String.IsNullOrWhiteSpace)

let invalidArgIfNotGreaterThanZero =
    invalidArgIf ((>=) 0)

let invalidArgIfLessThanZero =
    invalidArgIf ((>) 0)

let invalidArgIfDefault msg =
    invalidArgIf (fun (arg) -> arg = Unchecked.defaultof<'a>) msg

let reraize ex =
    (ExceptionDispatchInfo.Capture ex).Throw()

// Mix

let asyncDelay delay work =
    async {
        do! Async.Sleep delay
        work()
    } |> Async.StartImmediate

let signSafeMod dividend divisor =
    let modulo = dividend % divisor
    if modulo < 0
    then modulo + divisor
    else modulo