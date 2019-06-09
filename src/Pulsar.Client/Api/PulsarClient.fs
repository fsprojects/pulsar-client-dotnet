namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal
open System.Net.Http

type PulsarClient(config: PulsarClientConfiguration)  =    
   
    let lookupSerivce =
        if (config.ServiceUrl.StartsWith("http"))
        then HttpLookupService(config) :> ILookupService
        else BinaryProtoLookupService(config) :> ILookupService

    member this.SubscribeAsync consumerConfig =
        this.SingleTopicSubscribeAsync consumerConfig

    member this.GetPartitionedTopicMetadata topicName =
        lookupSerivce.GetPartitionedTopicMetadata topicName

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic
            if (metadata.Partitions > 1) 
            then
                return Consumer()
            else
                return Consumer()                
        }

    member this.CreateProducerAsync (producerConfig: ProducerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata producerConfig.Topic
            if (metadata.Partitions > 1) 
            then
                return Producer(config, producerConfig)
            else
                return Producer(config, producerConfig)                
        }