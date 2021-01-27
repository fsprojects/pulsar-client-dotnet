namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests

module PulsarClientBuilderTests =

    let private builder() =
        PulsarClientBuilder()

    let configure builderF builder =
        fun() ->  builder |> builderF |> ignore

    [<Tests>]
    let tests =

        testList "PulsarClientBuilderTests" [

            test "WithServiceUrl throws an exception for blank url" {
                let checkUrl url =
                    builder()
                    |> configure(fun b -> b.ServiceUrl url)
                    |> Expect.throwsWithMessage<ArgumentException> "ServiceUrl must not be blank."
                [null; ""; " "] |> List.iter checkUrl
            }

            test "MaxNumberOfRejectedRequestPerConnection throws an exception for negative value" {
                builder()
                |> configure(fun b -> b.MaxNumberOfRejectedRequestPerConnection -1)
                |> Expect.throwsWithMessage<ArgumentException> "MaxNumberOfRejectedRequestPerConnection can't be negative"
            }

            test "Build throws an exception if ServiceUrl is empty" {
                fun() -> builder().BuildAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "Service Url needs to be specified on the PulsarClientBuilder object."
            }

        ]