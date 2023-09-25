module Pulsar.Client.UnitTests.Common.Hash

open System.IO
open Expecto
open Expecto.Flip
open Microsoft.IO
open Pulsar.Client.Common

[<Tests>]
let murmurTests =

    testList "Murmur3Hash" [
        test "Murmur3Hash test1" {
            let input = "I will not buy this record, it is scratched."
            let hash = MurmurHash3.Hash(input)
            Expect.equal "" 684731290 hash
        }

        test "Murmur3Hash test2" {
            let input = "Hello world"
            let hash = MurmurHash3.Hash(input)
            Expect.equal "" 764499724 hash
        }

        test "Murmur3Hash test3" {
            let input = "Съешь ещё этих мягких французских булок, да выпей чаю"
            let hash = MurmurHash3.Hash(input)
            Expect.equal "" 1885574967 hash
        }

        testProperty "Hashes work same" <| fun (a: string) ->
            let hash1 = MurmurHash3.Hash(a)
            let hash2 = MurmurHash3.HashWithAllocs(a)
            Expect.equal "" hash1 hash2
    ]

[<Tests>]
let crc32cTests =

    testList "CRC32Hash" [

        test "CRC32Hash RMS" {
            let input = "Съешь ещё этих мягких французских булок, да выпей чаю" |> System.Text.Encoding.UTF8.GetBytes
            let recycl = MemoryStreamManager.GetStream() :?> RecyclableMemoryStream
            recycl.Write(input)
            recycl.Seek(0L, SeekOrigin.Begin) |> ignore
            let hash = CRC32C.GetForRMS(recycl, input.Length)
            Expect.equal "" 2789859932u hash
        }

        test "CRC32Hash test4" {
            let input = "Съешь ещё этих мягких французских булок, да выпей чаю" |> System.Text.Encoding.UTF8.GetBytes
            let stream = new MemoryStream(input, 0, input.Length, false, true)
            let hash = CRC32C.GetForMS(stream, input.Length)
            Expect.equal "" 2789859932u hash
        }
    ]

