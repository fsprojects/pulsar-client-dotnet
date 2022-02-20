module Pulsar.Client.UnitTests.Common.ToolsTests

open System
open System.Collections
open System.Threading.Tasks
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
        
        test "Reverse Over 64 elements" {
            let ba = BitArray(65)
            ba.Set(63, true)
            ba.Set(64, true)
            let result = fromLongArray [| -9223372036854775808L; 1L|] 65
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        test "Reverse 64 elements" {
            let ba = BitArray(64)
            ba.Set(1, true)
            ba.Set(63, true)
            let result = fromLongArray [|-9223372036854775806L|] 64
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        test "Reverse Less than 64 elements" {
            let ba = BitArray(3)
            ba.Set(1, true)
            ba.Set(2, true)
            let result = fromLongArray [|6L|] 3
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        testTask "Async delay shouldn't crash process" {
            let x = true
            let f = fun () -> failwith "failure"
            asyncDelayMs 20 f
            do! Task.Delay 100
            do! Task.Delay 100
            Console.WriteLine(x)
            Expect.isTrue "" x
        }
    ]
    
 