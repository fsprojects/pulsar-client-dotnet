namespace Pulsar.Client.Api

open Pulsar.Client.Common
open FSharp.UMX

type ConsumerBuilder private (client: PulsarClient, config: ConsumerConfiguration) =

    let verify(config : ConsumerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> throwIfDefault (fun() ->  ConsumerException("Topic name must be set on the producer builder.")))
        |> checkValue
            (fun c ->
                c.SubscriptionName
                |> throwIfBlankString (fun() -> ConsumerException("Subscription name name must be set on the producer builder.")))

    new(client: PulsarClient) = ConsumerBuilder(client, ConsumerConfiguration.Default)

    member this.Topic topic =
        ConsumerBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> TopicName })

    member this.ConsumerName name =
        ConsumerBuilder(
            client,
            { config with
                ConsumerName = name |> invalidArgIfBlankString "Consumer name must not be blank." })

    member this.SubscriptionName subscriptionName =
        ConsumerBuilder(
            client,
            { config with
                SubscriptionName = subscriptionName |> invalidArgIfBlankString "Subscription name must not be blank." })

    member this.SubscriptionType subscriptionType =
        ConsumerBuilder(
            client,
            { config with
                SubscriptionType = subscriptionType  })

    member this.ReceiverQueueSize receiverQueueSize =
        ConsumerBuilder(
            client,
            { config with
                ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  })

    member this.SubscriptionInitialPosition subscriptionInitialPosition =
        ConsumerBuilder(
            client,
            { config with
                SubscriptionInitialPosition = subscriptionInitialPosition  })

    member this.SubscribeAsync() =
        config
        |> verify
        |> client.SubscribeAsync