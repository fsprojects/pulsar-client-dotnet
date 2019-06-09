namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System

type Producer(config: PulsarClientConfiguration, producerConfig: ProducerConfiguration) =    
    let producerId = Generators.getNextProducerId()
    let serviceNameResolver = ServiceNameResolver(config)

    member this.SendAsync (msg: byte[]) =
         task {
            let payload = msg;
            let metadata = new MessageMetadata()
            let sequenceId = Generators.getNextSequenceId()
            let command = 
                Commands.newSend producerId sequenceId 1 ChecksumType.None metadata payload
                |> ReadOnlyMemory<byte>
            let address = serviceNameResolver.ResolveHost()
            let! conn = ConnectionPool.getConnectionAsync address
            // todo find where to apply producer config
            let! flushResult = conn.Output.WriteAsync(command)            
            return { LedgerId = % 0L; EntryId = % 0L; PartitionIndex = 0 }
        }
    member this.AcknowledgeAsync msg =
        Task.FromResult()
