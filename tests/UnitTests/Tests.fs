module Tests

open Expecto

[<Tests>]

let tests =     
    testList "samples" [
        testCase "unit test expecto works" <| fun () ->
            Expect.isTrue true "always true"

        testProperty "addition is commutative" <| fun a b ->
            a + b = b + a
    ]
