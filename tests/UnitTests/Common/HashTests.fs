module Pulsar.Client.UnitTests.Common.Hash

open Expecto
open Expecto.Flip
open Pulsar.Client.Common

[<Tests>]
let tests =

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

