namespace Pulsar.Client.Api

open Pulsar.Client.Common
open FSharp.UMX

type PulsarClientConfiguration =
    {
        ServiceUrl: string
    }
    static member Default =
        {
            ServiceUrl = ""
        }

type ConsumerConfiguration =
    {
        Topic: TopicName
        ConsumerName: string
        SubscriptionName: string
        SubscriptionType: SubscriptionType
        ReceiverQueueSize: int
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = 1000
        }

type ProducerConfiguration =
    {
        Topic: TopicName
        ProducerName: string
        MaxPendingMessages: int
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ProducerName = ""
            MaxPendingMessages = 1000
        }
