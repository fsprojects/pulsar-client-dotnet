namespace Pulsar.Client.UnitTests.Api

open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open System

module ProducerBuilderTests =

    let private builder() =
        ProducerBuilder(PulsarClient(PulsarClientConfiguration.Default))

    let configure builderF builder =
        fun() ->  builder |> builderF |> ignore

    [<Tests>]
    let tests =

        testList "ProducerBuilderTests" [

            test "Topic throws an exception for blank topic" {
                let checkTopic topic =
                    builder()
                    |> configure(fun b -> b.Topic topic)
                    |> Expect.throwsT<ArgumentException> ""

                [null; ""; " "] |> List.iter checkTopic
            }

            test "CreateAsync throws an exception if Topic is blank" {
                fun() -> builder().CreateAsync() |> ignore
                |> Expect.throwsT<ProducerException> ""
            }

        ]