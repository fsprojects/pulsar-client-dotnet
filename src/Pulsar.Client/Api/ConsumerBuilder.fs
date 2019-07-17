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

    member __.Topic topic =
        ConsumerBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> TopicName })

    member __.ConsumerName name =
        ConsumerBuilder(
            client,
            { config with
                ConsumerName = name |> invalidArgIfBlankString "Consumer name must not be blank." })

    member __.SubscriptionName subscriptionName =
        ConsumerBuilder(
            client,
            { config with
                SubscriptionName = subscriptionName |> invalidArgIfBlankString "Subscription name must not be blank." })

    member __.SubscriptionType subscriptionType =
        ConsumerBuilder(
            client,
            { config with
                SubscriptionType = subscriptionType  })

    member __.ReceiverQueueSize receiverQueueSize =
        ConsumerBuilder(
            client,
            { config with
                ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  })

    member __.SubscribeAsync() =
        config
        |> verify
        |> client.SubscribeAsync