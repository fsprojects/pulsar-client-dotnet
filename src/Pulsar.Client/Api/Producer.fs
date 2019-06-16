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

type Producer(producerConfig: ProducerConfiguration, lookup: BinaryLookupService) =    
    let producerId = Generators.getNextProducerId()
    let connectionHandler = ConnectionHandler()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let mutable clientCnx: ClientCnx option = None
    let partitionIndex = -1

    do connectionHandler.ConnectionOpened.Add(fun conn -> 
        clientCnx <- Some { Connection = conn; ProducerId = producerId; ConsumerId = %0L }
    )

    do connectionHandler.MessageDelivered.Add(fun sendAck -> 
        match messages.TryGetValue(sendAck.SequenceId) with
        | true, tsc ->
            tsc.SetResult({
                LedgerId = sendAck.LedgerId
                EntryId = sendAck.EntryId
                PartitionIndex = partitionIndex
            })
            messages.TryRemove(sendAck.SequenceId) |> ignore
        | _ -> ()
    )

    do connectionHandler.GrabCnx producerConfig.Topic lookup |> ignore

    member this.SendAsync (msg: byte[]) =
        if clientCnx.IsNone
        then failwith "Connection is not ready"
        else
            task {
                let payload = msg;
                let sequenceId = Generators.getNextSequenceId()
                let metadata = 
                    new MessageMetadata(
                        SequenceId = %sequenceId,
                        PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                        ProducerName = producerConfig.ProducerName,
                        UncompressedSize = (payload.Length |> uint32)
                    )
                let command = 
                    Commands.newSend producerId sequenceId 1 ChecksumType.No metadata payload
                    |> ReadOnlyMemory<byte>
                let! flushResult = clientCnx.Value.Connection.Output.WriteAsync(command)
                let tsc = TaskCompletionSource<MessageId>()
                if messages.TryAdd(sequenceId, tsc)
                then
                    return! tsc.Task
                else 
                    return failwith "Unable to add tsc"
            }
