namespace Pulsar.Client.Api

open Pulsar.Client.Common
open FSharp.UMX
open System
open FSharp.Control.Tasks.V2.ContextInsensitive

type ConsumerBuilder private (client: PulsarClient, config: ConsumerConfiguration) =

    [<Literal>]
    let MIN_ACK_TIMEOUT_MILLIS = 1000

    [<Literal>]
    let DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER = 30000.0

    let verify(config : ConsumerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the consumer builder.")
        |> checkValue
            (fun c ->
                c.SubscriptionName
                |> invalidArgIfBlankString "Subscription name must be set on the consumer builder.")
        |> checkValue
            (fun c ->
                invalidArgIfTrue (
                    c.ReadCompacted && (not c.Topic.IsPersistent ||
                        (c.SubscriptionType <> SubscriptionType.Exclusive && c.SubscriptionType <> SubscriptionType.Failover ))
                ) "Read compacted can only be used with exclusive of failover persistent subscriptions")

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

    member this.AckTimeout ackTimeout =
        ConsumerBuilder(
            client,
            { config with
                AckTimeout = ackTimeout |> invalidArgIf (fun arg ->
                   arg <> TimeSpan.Zero && arg < TimeSpan.FromMilliseconds(float MIN_ACK_TIMEOUT_MILLIS)) (sprintf "Ack timeout should be greater than %i ms" MIN_ACK_TIMEOUT_MILLIS)  })

    member this.AckTimeoutTickTime ackTimeoutTickTime =
        ConsumerBuilder(
            client,
            { config with
                AckTimeoutTickTime = ackTimeoutTickTime  })

    member this.AcknowledgementsGroupTime ackGroupTime =
        ConsumerBuilder(
            client,
            { config with
                AcknowledgementsGroupTime = ackGroupTime  })

    member this.ReadCompacted readCompacted =
        ConsumerBuilder(
            client,
            { config with
                ReadCompacted = readCompacted  })

    member this.NegativeAckRedeliveryDelay negativeAckRedeliveryDelay =
        ConsumerBuilder(
            client,
            { config with
                NegativeAckRedeliveryDelay = negativeAckRedeliveryDelay  })

    member __.DeadLettersPolicy (policy: DeadLettersPolicy) =

        let ackTimeoutTickTime =
            if config.AckTimeoutTickTime = TimeSpan.Zero
            then TimeSpan.FromMilliseconds(DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER)
            else config.AckTimeoutTickTime

        let getTopicName() = config.Topic.ToString()
        let getSubscriptionName() = config.SubscriptionName
        let createProducer deadLetterTopic = ProducerBuilder(client).Topic(deadLetterTopic).CreateAsync().Result
        let deadLettersProcessor = DeadLettersProcessor(policy, getTopicName, getSubscriptionName, createProducer)

        ConsumerBuilder(
            client,
            { config with
                AckTimeoutTickTime = ackTimeoutTickTime
                DeadLettersProcessor = deadLettersProcessor |> Some })

    member this.SubscribeAsync() =
        config
        |> verify
        |> client.SubscribeAsync