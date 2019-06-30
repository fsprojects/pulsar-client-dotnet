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

            test "SubscribeAsync throws an exception if Topic is blank" {
                let builder' = builder().SubscriptionName("subscription-name")

                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ConsumerException> "Topic name must be set on the producer builder."
            }

            test "SubscribeAsync throws an exception if SubscriptionName is blank" {
                let builder' = builder().Topic("topic-name")
                
                fun() -> builder'.SubscribeAsync() |> ignore
                |> Expect.throwsWithMessage<ConsumerException>
                    "Subscription name name must be set on the producer builder."
            }

        ]