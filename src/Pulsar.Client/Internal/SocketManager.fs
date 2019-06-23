module Pulsar.Client.Internal.SocketManager

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Collections.Concurrent
open System.Net
open System.Buffers
open System.Buffers.Binary
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open System
open System.IO.Pipelines
open Pulsar.Client.Api
open pulsar.proto
open ProtoBuf
open System.IO
open FSharp.UMX
open System.Reflection

let clientVersion = "Pulsar.Client v" + Assembly.GetExecutingAssembly().GetName().Version.ToString()
let protocolVersion = 
    ProtocolVersion.GetValues(typeof<ProtocolVersion>) 
    :?> ProtocolVersion[] 
    |> Array.last

let connections = ConcurrentDictionary<EndPoint, Lazy<Task<SocketConnection>>>()
let requests = ConcurrentDictionary<RequestId, TaskCompletionSource<obj>>()
let consumers = ConcurrentDictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
let producers = ConcurrentDictionary<ProducerId, MailboxProcessor<ProducerMessage>>()


type PulsarCommands =
    | XCommandConnected of CommandConnected * SequencePosition
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse * SequencePosition
    | XCommandSendReceipt of CommandSendReceipt * SequencePosition
    | XCommandMessage of CommandMessage * SequencePosition
    | IncompleteCommand
    | InvalidCommand of Exception


let tryParse (buffer: ReadOnlySequence<byte>) =
    let array = buffer.ToArray()
    let sp = ReadOnlySpan(array)
    if (sp.Length >= 8)
    then
        let totalength = BinaryPrimitives.ReadInt32BigEndian(sp)
        let frameLength = totalength + 4
        if (totalength <= sp.Length)
        then
            let commandLength = BinaryPrimitives.ReadInt32BigEndian(sp.Slice(4))
            let msgStream =  new MemoryStream(sp.Slice(8,commandLength).ToArray())
            try
                let command = Serializer.Deserialize<BaseCommand>(msgStream)
                match command.``type`` with
                | BaseCommand.Type.Connected -> 
                    XCommandConnected (command.Connected, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.PartitionedMetadataResponse -> 
                    XCommandPartitionedTopicMetadataResponse (command.partitionMetadataResponse, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.SendReceipt -> 
                    XCommandSendReceipt (command.SendReceipt, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Message -> 
                    XCommandMessage (command.Message, buffer.GetPosition(int64 frameLength))
                | _ -> 
                    InvalidCommand (Exception("Unknown command type"))
            with
            | ex ->
                InvalidCommand ex
        else    
            IncompleteCommand
    else
        IncompleteCommand

let private readSocket (conn: SocketConnection) (tsc: TaskCompletionSource<SocketConnection>) =
    task {
        let mutable continueLooping = true
        let reader = conn.Input
        while continueLooping do
            let! result = reader.ReadAsync()
            let buffer = result.Buffer
            if result.IsCompleted
            then                
                continueLooping <- false
            else
                match tryParse buffer with
                | XCommandConnected (cmd, consumed) ->
                    //TODO check server protocol version
                    tsc.SetResult(conn)
                    reader.AdvanceTo(consumed)
                | XCommandPartitionedTopicMetadataResponse (cmd, consumed) ->
                    let requestId = %cmd.RequestId
                    let tsc = requests.[requestId]
                    tsc.SetResult({ Partitions = cmd.Partitions })  
                    requests.TryRemove(requestId) |> ignore
                    reader.AdvanceTo(consumed)
                | XCommandSendReceipt (cmd, consumed) ->
                    let producerMb = producers.[%cmd.ProducerId]
                    producerMb.Post(SendReceipt cmd)   
                    reader.AdvanceTo(consumed)
                | XCommandMessage (cmd, consumed) ->
                    let consumerEvent = consumers.[%cmd.ConsumerId]
                    // TODO handle real messages
                    consumerEvent.Post(AddMessage { MessageId = null; Payload = [||] })        
                    reader.AdvanceTo(consumed)
                | IncompleteCommand ->
                    reader.AdvanceTo(buffer.Start, buffer.End)
                | InvalidCommand ex ->
                    raise ex
    }

type Payload = SocketConnection*ReadOnlyMemory<byte>
type SocketMessage = Payload * AsyncReplyChannel<FlushResult>

let sendMb = MailboxProcessor<SocketMessage>.Start(fun inbox ->
    let rec loop () =
        async {
            let! ((connection, payload), replyChannel) = inbox.Receive()
            let! flushResult = connection.Output.WriteAsync(payload).AsTask() |> Async.AwaitTask
            replyChannel.Reply(flushResult)
            return! loop ()             
        }
    loop ()
)


let private connect address =
    task {
        let! connection = SocketConnection.ConnectAsync(address)
        let initialConnectionTsc = TaskCompletionSource<SocketConnection>()
        let listener = Task.Run(fun() -> (readSocket connection initialConnectionTsc).Wait())
        let connectPayload = 
            Commands.newConnect clientVersion protocolVersion
            |> ReadOnlyMemory
        let! flushResult = sendMb.PostAndAsyncReply(fun replyChannel -> (connection, connectPayload), replyChannel)
        return! initialConnectionTsc.Task
    }    

let getConnection (broker: Broker) =   
    connections.GetOrAdd(broker.PhysicalAddress, fun(address) -> 
        lazy connect address).Value
               
let registerProducer (broker: Broker) (producerId: ProducerId) (producerMb: MailboxProcessor<ProducerMessage>) =
    let connection = getConnection broker
    producers.TryAdd(producerId, producerMb) |> ignore
    connection

let registerConsumer (broker: Broker) (consumerId: ConsumerId) (consumerMb: MailboxProcessor<ConsumerMessage>) =
    let connection = getConnection broker
    consumers.TryAdd(consumerId, consumerMb) |> ignore
    connection

let send payload = 
    sendMb.PostAndAsyncReply(fun replyChannel -> payload, replyChannel)

let sendAndWaitForReply reqId payload = 
    task {
        let! flushResult = sendMb.PostAndAsyncReply(fun replyChannel -> payload, replyChannel)
        let tsc = TaskCompletionSource()
        if requests.TryAdd(reqId, tsc) |> not
        then tsc.SetException(Exception("Duplicate request"))       
        return! tsc.Task
    }