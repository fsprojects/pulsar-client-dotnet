namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Collections.Concurrent

type ProducerException(message) =
    inherit Exception(message)

type Producer private (producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =    
    let producerId = Generators.getNextProducerId()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let partitionIndex = -1
    let mutable connectionHandler: ConnectionHandler = Unchecked.defaultof<ConnectionHandler>

    member this.SendAsync (msg: byte[]) =
        task {
            let payload = msg;
            let sequenceId = Generators.getNextSequenceId()
            let metadata = 
                MessageMetadata (
                    SequenceId = %sequenceId,
                    PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                    ProducerName = producerConfig.ProducerName,
                    UncompressedSize = (payload.Length |> uint32)
                )
            let command = 
                Commands.newSend producerId sequenceId 1 ChecksumType.No metadata payload
                |> ReadOnlyMemory<byte>
            let! flushResult = connectionHandler.Send(command)
            let tsc = TaskCompletionSource<MessageId>()
            if messages.TryAdd(sequenceId, tsc)
            then
                return! tsc.Task
            else 
                return failwith "Unable to add tsc"
        }

    member private __.InitConnectionHandler(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let! ch = ConnectionHandler.Init(producerConfig.Topic, lookup)
            connectionHandler <- ch
        }

    static member Init(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = Producer(producerConfig, lookup)
            do! producer.InitConnectionHandler(producerConfig, lookup)
            return producer
        }
        