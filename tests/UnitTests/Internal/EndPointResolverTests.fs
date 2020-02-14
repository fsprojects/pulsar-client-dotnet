module Pulsar.Client.UnitTests.Internal.EndPointResolverTests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests
open System.Net

[<Tests>]
let tests =

    let checkEndPointBy (uri : Uri ) (endPoint : DnsEndPoint) =
        Expect.equal "" uri.Host endPoint.Host
        Expect.equal "" uri.Port endPoint.Port

    testList "EndPointResolver" [

        test "Resolver throws exception for empty address list" {
            Expect.throwsWithMessage<ArgumentException>
                "Addresses list could not be empty. (Parameter 'addresses')"
                (fun() -> EndPointResolver([]) |> ignore )
        }

        test "Resolver works with single address" {
            let address = Uri("pulsar://host1:6650")
            let resolver = EndPointResolver([address])

            resolver.Resolve() |> checkEndPointBy address
            resolver.Resolve() |> checkEndPointBy address
        }

        test "Resolver uses round robin scheme" {
            let address1 = Uri("pulsar://host1:6650")
            let address2 = Uri("pulsar://host2:6650")
            let address3 = Uri("pulsar://host3:6650")
            let resolver = EndPointResolver([address1; address2; address3])

            resolver.Resolve() |> checkEndPointBy address1
            resolver.Resolve() |> checkEndPointBy address2
            resolver.Resolve() |> checkEndPointBy address3
            resolver.Resolve() |> checkEndPointBy address1
            resolver.Resolve() |> checkEndPointBy address2
        }
    ]