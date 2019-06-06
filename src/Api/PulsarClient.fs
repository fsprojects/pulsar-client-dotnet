namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextSensitive
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

    member this.CreateProducerAsync (consumerConfig: ProducerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic
            if (metadata.Partitions > 1) 
            then
                return Producer()
            else
                return Producer()                
        }