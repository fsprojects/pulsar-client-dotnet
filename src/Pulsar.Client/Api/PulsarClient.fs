namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal

open System

type PulsarClientException(message) =
    inherit Exception(message)

type PulsarClient(config: PulsarClientConfiguration) =

    let lookupSerivce = BinaryLookupService(config)

    member this.SubscribeAsync consumerConfig =
        this.SingleTopicSubscribeAsync consumerConfig

    member this.GetPartitionedTopicMetadata topicName =
        lookupSerivce.GetPartitionedTopicMetadata topicName

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic
            if (metadata.Partitions > 1u)
            then
                return! Consumer.Init(consumerConfig, lookupSerivce)
            else
                return! Consumer.Init(consumerConfig, lookupSerivce)
        }

    member this.CreateProducerAsync (producerConfig: ProducerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata producerConfig.Topic
            if (metadata.Partitions > 1u)
            then
                return! Producer.Init(producerConfig, lookupSerivce)
            else
                return! Producer.Init(producerConfig, lookupSerivce)
        }