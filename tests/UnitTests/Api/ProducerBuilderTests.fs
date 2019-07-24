namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests

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
                    |> Expect.throwsWithMessage<ArgumentException> "Topic must not be blank."

                [null; ""; " "] |> List.iter checkTopic
            }

            test "ProducerName throws an exception for blank producer name" {
                let checkProducerName producerName =
                    builder()
                    |> configure(fun b -> b.ProducerName producerName)
                    |> Expect.throwsWithMessage<ArgumentException> "ProducerName must not be blank."

                [null; ""; " "] |> List.iter checkProducerName
            }

            test "MaxPendingMessages throws an exception for non positive integer" {
                let checkMaxPendingMessages maxPendingMessages =
                    builder()
                    |> configure(fun b -> b.MaxPendingMessages maxPendingMessages)
                    |> Expect.throwsWithMessage<ArgumentException> "MaxPendingMessages needs to be greater than 0."

                [-1; 0] |> List.iter checkMaxPendingMessages
            }

            test "CreateAsync throws an exception if Topic is blank" {
                fun() -> builder().CreateAsync() |> ignore
                |> Expect.throwsWithMessage<ProducerException> "Topic name must be set on the producer builder."
            }

            test "BatchingMaxMessages throws an exception for negative integer" {
                let checkBatchingMaxMessages batchingMaxMessages =
                    builder()
                    |> configure(fun b -> b.BatchingMaxMessages batchingMaxMessages)
                    |> Expect.throwsWithMessage<ArgumentException> "BatchingMaxMessages needs to be non negative integer."

                -1 |> checkBatchingMaxMessages
            }

        ]