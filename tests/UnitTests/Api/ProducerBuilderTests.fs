namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests
open Pulsar.Client.Common

module ProducerBuilderTests =

    let private builder() =
        PulsarClient({ PulsarClientConfiguration.Default with ServiceAddresses = [ Uri("pulsar://localhost:6650") ] }).NewProducer()

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
                    |> Expect.throwsWithMessage<ArgumentException> "MaxPendingMessages needs to be >= 0."

                -1 |> checkMaxPendingMessages
            }
            
            test "MaxPendingMessagesActossPartitions throws an exception for non positive integer" {
                let checkMaxPendingMessages maxPendingMessages =
                    builder()
                    |> configure(fun b -> b.MaxPendingMessagesAcrossPartitions maxPendingMessages)
                    |> Expect.throwsWithMessage<ArgumentException> "MaxPendingMessagesAcrossPartitions needs to be >= 0."

                -1 |> checkMaxPendingMessages
            }

            test "CreateAsync throws an exception if Topic is blank" {
                fun() -> builder().CreateAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException> "Topic name must be set on the producer builder."
            }

            test "BatchingMaxMessages throws an exception for negative integer" {
                let checkBatchingMaxMessages batchingMaxMessages =
                    builder()
                    |> configure(fun b -> b.BatchingMaxMessages batchingMaxMessages)
                    |> Expect.throwsWithMessage<ArgumentException> "BatchingMaxMessages needs to be greater than 0."

                -1 |> checkBatchingMaxMessages
            }

            test "Custom routing mode throws an exception for null custom router" {
                fun() ->
                    builder()
                        .MessageRoutingMode(MessageRoutingMode.CustomPartition)
                        .Topic("Some topic")
                        .CreateAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException> "Valid router should be set with CustomPartition routing mode."
            }
        ]