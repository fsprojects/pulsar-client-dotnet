namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging

open System

type PulsarClientException(message) =
    inherit Exception(message)

type PulsarClient(config: PulsarClientConfiguration) =

    let lookupSerivce = BinaryLookupService(config)

    member this.SubscribeAsync consumerConfig =
        this.SingleTopicSubscribeAsync consumerConfig

    member this.GetPartitionedTopicMetadata topicName =
        lookupSerivce.GetPartitionedTopicMetadata topicName

    static member Logger
        with get () = Log.Logger
        and set (value) = Log.Logger <- value

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration) =
        task {
            Log.Logger.LogDebug("SingleTopicSubscribeAsync started")
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic.CompleteTopicName
            if (metadata.Partitions > 1u)
            then
                return! Consumer.Init(consumerConfig, SubscriptionMode.Durable, lookupSerivce)
            else
                return! Consumer.Init(consumerConfig, SubscriptionMode.Durable, lookupSerivce)
        }

    member this.CreateProducerAsync (producerConfig: ProducerConfiguration) =
        task {
            Log.Logger.LogDebug("CreateProducerAsync started")
            let! metadata = this.GetPartitionedTopicMetadata producerConfig.Topic.CompleteTopicName
            if (metadata.Partitions > 1u)
            then
                return! Producer.Init(producerConfig, lookupSerivce)
            else
                return! Producer.Init(producerConfig, lookupSerivce)
        }