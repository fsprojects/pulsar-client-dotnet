namespace Pulsar.Client.UnitTests.Common

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open Pulsar.Client.UnitTests

module NamespaceNameTests =

    [<Tests>]
    let tests =

        testList "NamespaceNameTests" [

            test "NamespaceName throws an exception for blank namespace name" {
                let checkNamespaceName ns =
                    fun() -> NamespaceName(ns) |> ignore
                    |> Expect.throwsWithMessage<ArgumentException> "Namespace name must not be blank."

                [null; ""; " "] |> List.iter checkNamespaceName
            }

            test "NamespaceName throws an exception for incorrect namespace name format" {
                let checkNamespaceName ns =
                    fun() -> NamespaceName(ns) |> ignore
                    |> Expect.throwsWithMessage<ArgumentException>
                        "Invalid namespace name format. Namespace name must be specified as '<tenant>/<namespace>'."

                checkNamespaceName "my-namespace"
            }

            test "Tenant should return correct value" {
                NamespaceName("public/default").Tenant
                |> Expect.equal "" "public"
            }

            test "LocalName should return correct value" {
                NamespaceName("public/default").LocalName
                |> Expect.equal "" "default"
            }

            test "ToString should return initial value" {
                NamespaceName("public/default").ToString()
                |> Expect.equal "" "public/default"
            }

        ]