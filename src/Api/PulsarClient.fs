namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextSensitive
open Pulsar.Client.Common

type PulsarClient(config: PulsarClientConfiguration)  =    
    member this.SubscribeAsync consumerConfig =
        this.SingleTopicSubscribeAsync consumerConfig
    member this.GetPartitionedTopicMetadata() =
        task {
            return {
                Partitions = 1
            }
        }

    member private this.SingleTopicSubscribeAsync consumerConfig =
        task {
            let! metadata = this.GetPartitionedTopicMetadata()
            if (metadata.Partitions > 1) 
            then
                return Consumer()
            else
                return Consumer()                
        }