module Tests

open System
open Expecto

[<Tests>]

let tests = 
    testList "samples" [
        testCase "integration test expecto works" <| fun () ->
            Expect.isTrue true "always true"
    ]
