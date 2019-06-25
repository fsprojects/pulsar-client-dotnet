namespace Pulsar.Client.UnitTests.Api

open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open System

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
                    |> configure(fun b -> b.WithServiceUrl url)
                    |> Expect.throwsT<ArgumentException> ""

                [null; ""; " "] |> List.iter checkUrl
            }

            test "Build throws an exception if ServiceUrl is blank" {
                fun() -> builder().Build() |> ignore
                |> Expect.throwsT<PulsarClientException> ""
            }

        ]