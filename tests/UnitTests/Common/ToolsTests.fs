module Pulsar.Client.UnitTests.Common.ToolsTests

open System.Collections
open Expecto
open Expecto.Flip
open Pulsar.Client.Common

[<Tests>]
let tests =

    testList "BitSetToLongArray" [
        test "Over 64 elements" {
            let ba = BitArray(65)
            ba.Set(63, true)
            ba.Set(64, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [-9223372036854775808L; 1L] result
        }
        
        test "64 elements" {
            let ba = BitArray(64)
            ba.Set(1, true)
            ba.Set(63, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [-9223372036854775806L] result
        }
        
        test "Less than 64 elements" {
            let ba = BitArray(3)
            ba.Set(1, true)
            ba.Set(2, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [6L] result
        }
    ]