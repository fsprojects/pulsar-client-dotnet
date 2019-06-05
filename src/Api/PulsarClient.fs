namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextSensitive
open Pulsar.Client.Internal
open System.Net.Http

type PulsarClient(config: PulsarClientConfiguration)  =    
    
    let httpClient = new HttpClient()

    member this.SubscribeAsync consumerConfig =
        this.SingleTopicSubscribeAsync consumerConfig

    member this.GetPartitionedTopicMetadata topicName =
        HttpLookupService.getPartitionedTopicMetadata httpClient config topicName

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic
            if (metadata.Partitions > 1) 
            then
                return Consumer()
            else
                return Consumer()                
        }