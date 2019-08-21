namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests

module ConsumerBuilderTests =

    let private builder() =
        ConsumerBuilder(PulsarClient(PulsarClientConfiguration.Default))

    let configure builderF builder =
        fun() ->  builder |> builderF |> ignore

    [<Tests>]
    let tests =

        testList "ConsumerBuilderTests" [

            test "Topic throws an exception for blank topic" {
                let checkTopic topic =
                    builder()
                    |> configure(fun b -> b.Topic topic)
                    |> Expect.throwsWithMessage<ArgumentException> "Topic must not be blank."

                [null; ""; " "] |> List.iter checkTopic
            }

            test "SubscriptionName throws an exception for blank subscriptionName" {
                let checkSubscriptionName subscriptionName =
                    builder()
                    |> configure(fun b -> b.SubscriptionName subscriptionName)
                    |> Expect.throwsWithMessage<ArgumentException> "Subscription name must not be blank."

                [null; ""; " "] |> List.iter checkSubscriptionName
            }

            test "ConsumerName throws an exception for blank consumerName" {
                let checkConsumerName consumerName =
                    builder()
                    |> configure(fun b -> b.ConsumerName consumerName)
                    |> Expect.throwsWithMessage<ArgumentException> "Consumer name must not be blank."

                [null; ""; " "] |> List.iter checkConsumerName
            }

            test "ReceiverQueueSize throws an exception for non-positive value" {
                let checkReceiverQueueSize receiverQueueSize =
                    builder()
                    |> configure(fun b -> b.ReceiverQueueSize receiverQueueSize)
                    |> Expect.throwsWithMessage<ArgumentException> "ReceiverQueueSize should be greater than 0."

                [-1; 0] |> List.iter checkReceiverQueueSize
            }

            test "SubscribeAsync throws an exception if Topic is blank" {
                let builder' = builder().SubscriptionName("subscription-name")

                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException> "Topic name must be set on the producer builder."
            }

            test "SubscribeAsync throws an exception if SubscriptionName is blank" {
                let builder' = builder().Topic("topic-name")

                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "Subscription name name must be set on the producer builder."
            }

            test "SubscribeAsync throws if ackTimeout is less than minimal" {
                (fun () ->
                    builder()
                        .Topic("topic-name")
                        .SubscriptionName("test-subscription")
                        .AckTimeout(TimeSpan.FromSeconds(0.5)) |> ignore)
                    |> Expect.throwsWithMessage<ArgumentException>  "Ack timeout should be greater than 1000 ms"
            }
        ]