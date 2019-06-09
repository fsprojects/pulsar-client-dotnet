namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System

type Producer(config: PulsarClientConfiguration, producerConfig: ProducerConfiguration, lookup: ILookupService) =    
    let producerId = Generators.getNextProducerId()

    member this.SendAsync (msg: byte[]) =
         task {
            let payload = msg;
            let metadata = new MessageMetadata()
            let sequenceId = Generators.getNextSequenceId()
            let command = 
                Commands.newSend producerId sequenceId 1 ChecksumType.None metadata payload
                |> ReadOnlyMemory<byte>
            let topicName = TopicName(producerConfig.Topic)
            let! broker = lookup.GetBroker(topicName)
            let! conn = ConnectionPool.getConnectionAsync broker
            let! flushResult = conn.Output.WriteAsync(command)            
            return { LedgerId = % 0L; EntryId = % 0L; PartitionIndex = topicName.PartitionInex }
        }
    member this.AcknowledgeAsync msg =
        Task.FromResult()
