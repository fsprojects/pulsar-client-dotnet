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
        AckTimeout: TimeSpan
        TickDuration: TimeSpan
        AcknowledgementsGroupTime: TimeSpan
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = 1000
            SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
            AckTimeout = TimeSpan.Zero
            TickDuration = TimeSpan.FromMilliseconds(1000.0)
            AcknowledgementsGroupTime = TimeSpan.FromMilliseconds(100.0)
        }

type ProducerConfiguration =
    {
        Topic: TopicName
        ProducerName: string
        MaxPendingMessages: int
        BatchingEnabled: bool
        MaxMessagesPerBatch: int
        MaxBatchingPublishDelay: TimeSpan
        SendTimeout: TimeSpan
        BatcherBuilder : IBatcherBuilder
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ProducerName = ""
            MaxPendingMessages = 1000
            BatchingEnabled = true
            MaxMessagesPerBatch = 1000
            MaxBatchingPublishDelay = TimeSpan.FromMilliseconds(1.)
            SendTimeout = TimeSpan.FromMilliseconds(30000.0)
            BatcherBuilder = BatcherBuilder()
        }
