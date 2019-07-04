namespace Pulsar.Client.Api

open Pulsar.Client.Common

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
        Topic: string
        ConsumerName: string
        SubscriptionName: string
        SubscriptionType: SubscriptionType
        ReceiverQueueSize: int
    }
    static member Default =
        {
            Topic = ""
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = 1000
        }

type ProducerConfiguration =
    {
        Topic: string
        ProducerName: string
    }
    static member Default =
        {
            Topic = ""
            ProducerName = ""
        }
