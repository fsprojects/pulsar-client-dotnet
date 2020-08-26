namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests
open Pulsar.Client.Common

module ConsumerBuilderTests =

    let private builder() =
        PulsarClient({ PulsarClientConfiguration.Default with ServiceAddresses = [ Uri("pulsar://localhost:6650") ] }).NewConsumer()

    let configure builderF builder =
        fun() ->  builder |> builderF |> ignore

    [<Tests>]
    let tests =

        testList "ConsumerBuilderTests" [

            test "Topic throws an exception for blank topic" {
                let checkTopic topic =
                    builder()
                    |> configure(fun b -> b.Topic topic)
                    |> Expect.throwsWithMessage<ArgumentException> "Topic must not be blank"

                [null; ""; " "] |> List.iter checkTopic
            }

            test "SubscriptionName throws an exception for blank subscriptionName" {
                let checkSubscriptionName subscriptionName =
                    builder()
                    |> configure(fun b -> b.SubscriptionName subscriptionName)
                    |> Expect.throwsWithMessage<ArgumentException> "Subscription name must not be blank"

                [null; ""; " "] |> List.iter checkSubscriptionName
            }

            test "ConsumerName throws an exception for blank consumerName" {
                let checkConsumerName consumerName =
                    builder()
                    |> configure(fun b -> b.ConsumerName consumerName)
                    |> Expect.throwsWithMessage<ArgumentException> "Consumer name must not be blank"

                [null; ""; " "] |> List.iter checkConsumerName
            }

            test "ReceiverQueueSize throws an exception for negative value" {
                let checkReceiverQueueSize receiverQueueSize =
                    builder()
                    |> configure(fun b -> b.ReceiverQueueSize receiverQueueSize)
                    |> Expect.throwsWithMessage<ArgumentException> "ReceiverQueueSize can't be negative."

                -1 |> checkReceiverQueueSize
            }

            test "SubscribeAsync throws an exception if Topic is blank" {
                let builder' = builder().SubscriptionName("subscription-name")

                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException> "Topic name must be set on the consumer builder"
            }

            test "SubscribeAsync throws an exception if SubscriptionName is blank" {
                let builder' = builder().Topic("topic-name")

                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "Subscription name must be set on the consumer builder"
            }

            test "AckTimeout throws if ackTimeout is less than minimal" {
                (fun () ->
                    builder()
                        .Topic("topic-name")
                        .SubscriptionName("test-subscription")
                        .AckTimeout(TimeSpan.FromSeconds(0.5)) |> ignore)
                    |> Expect.throwsWithMessage<ArgumentException>  "Ack timeout should be greater than 1000 ms"
            }

            test "SubscribeAsync throws if compacted and shared" {
                let checkReadCompacted subscription =
                    (fun () ->
                        builder()
                            .Topic("topic-name")
                            .SubscriptionName("test-subscription")
                            .SubscriptionType(subscription)
                            .ReadCompacted(true)
                            .SubscribeAsync()|> ignore)
                        |> Expect.throwsWithMessage<ArgumentException>  "Read compacted can only be used with exclusive or failover persistent subscriptions"
                [SubscriptionType.Shared; SubscriptionType.KeyShared] |> List.iter checkReadCompacted
            }

            test "SubscribeAsync throws if compacted and non-persistent topic" {
                (fun () ->
                    builder()
                        .Topic("non-persistent://my-tenant/my-namespace/my-topic")
                        .SubscriptionName("test-subscription")
                        .ReadCompacted(true)
                        .SubscribeAsync()|> ignore)
                    |> Expect.throwsWithMessage<ArgumentException>  "Read compacted can only be used with exclusive or failover persistent subscriptions"
            }
            
            test "PriorityLevel throws an exception for negative value" {
                let checkPriorityLevel priorityLevel =
                    builder()
                    |> configure(fun b -> b.PriorityLevel priorityLevel)
                    |> Expect.throwsWithMessage<ArgumentException> "PriorityLevel can't be negative."

                -1 |> checkPriorityLevel
            }
        ]
