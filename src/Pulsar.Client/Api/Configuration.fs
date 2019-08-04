namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System

type PulsarClientConfiguration =
    {
        ServiceUrl: string
        OperationTimeout: TimeSpan
    }
    static member Default =
        {
            ServiceUrl = ""
            OperationTimeout = TimeSpan.FromMilliseconds(30000.0)
        }

type ConsumerConfiguration =
    {
        Topic: TopicName
        ConsumerName: string
        SubscriptionName: string
        SubscriptionType: SubscriptionType
        ReceiverQueueSize: int
        SubscriptionInitialPosition: SubscriptionInitialPosition
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = 1000
            SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
        }

type ProducerConfiguration =
    {
        Topic: TopicName
        ProducerName: string
        MaxPendingMessages: int
        SendTimeout: TimeSpan
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ProducerName = ""
            MaxPendingMessages = 1000
            SendTimeout = TimeSpan.FromMilliseconds(30000.0)
        }
