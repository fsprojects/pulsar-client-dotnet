namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Collections.Concurrent

type Producer(producerConfig: ProducerConfiguration, lookup: ILookupService) =    
    let producerId = Generators.getNextProducerId()
    let connectionHandler = ConnectionHandler()
    let messages = ConcurrentDictionary<SequenceId, TaskCompletionSource<MessageId>>()
    let mutable clientCnx: ClientCnx option = None
    let partitionIndex = 0

    do connectionHandler.ConnectionOpened.Add(fun conn -> 
        clientCnx <- Some { Connection = conn; ProducerId = producerId }
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
                let metadata = new MessageMetadata()
                let sequenceId = Generators.getNextSequenceId()
                let command = 
                    Commands.newSend producerId sequenceId 1 ChecksumType.No metadata payload
                    |> ReadOnlyMemory<byte>
                let! flushResult = clientCnx.Value.Connection.Output.WriteAsync(command)
                let tsc = TaskCompletionSource<SendAck>()
                return! tsc.Task
            }
