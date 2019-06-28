module internal Pulsar.Client.Internal.SocketManager

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Collections.Concurrent
open System.Net
open System.Buffers
open System.Buffers.Binary
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open System
open Microsoft.Extensions.Logging
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


type PulsarTypes =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult

let connections = ConcurrentDictionary<EndPoint, Lazy<Task<Connection>>>()
let requests = ConcurrentDictionary<RequestId, TaskCompletionSource<PulsarTypes>>()
let consumers = ConcurrentDictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
let producers = ConcurrentDictionary<ProducerId, MailboxProcessor<ProducerMessage>>()


type PulsarCommands =
    | XCommandConnected of CommandConnected * SequencePosition
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse * SequencePosition
    | XCommandSendReceipt of CommandSendReceipt * SequencePosition
    | XCommandMessage of CommandMessage * SequencePosition
    | XCommandPing of CommandPing * SequencePosition
    | XCommandLookupTopic of CommandLookupTopicResponse * SequencePosition
    | IncompleteCommand
    | InvalidCommand of Exception


type Payload = Connection*SerializedPayload
type SocketMessage = 
    | SocketMessageWithReply of Payload * AsyncReplyChannel<unit>
    | SocketMessageWithoutReply of Payload

let sendSerializedPayload ((connection, serializedPayload): Payload ) = 
    task {
        let (conn, streamWriter) = connection
        let task = streamWriter |> serializedPayload         
               
        do! task
        if (not conn.Socket.Connected)
        then
            Log.Logger.LogWarning("Socket was disconnected")
            consumers |> Seq.iter (fun (kv) -> kv.Value.Post(ConsumerMessage.Disconnected (connection, kv.Value)))
            producers |> Seq.iter (fun (kv) -> kv.Value.Post(ProducerMessage.Disconnected (connection, kv.Value)))
    }

let sendMb = MailboxProcessor<SocketMessage>.Start(fun inbox ->    
    let rec loop () =
        async {
            match! inbox.Receive() with
            | SocketMessageWithReply (payload, replyChannel) ->
                Log.Logger.LogDebug("Sending payload with reply")
                do! sendSerializedPayload payload |> Async.AwaitTask
                // TODO handle failure properly
                replyChannel.Reply()
            | SocketMessageWithoutReply payload ->
                Log.Logger.LogDebug("Sending payload without reply")
                do! sendSerializedPayload payload |> Async.AwaitTask
            return! loop ()             
        }
    loop ()
)


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
                Log.Logger.LogDebug("Got message of type {0}", command.``type``)
                match command.``type`` with
                | BaseCommand.Type.Connected -> 
                    XCommandConnected (command.Connected, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.PartitionedMetadataResponse -> 
                    XCommandPartitionedTopicMetadataResponse (command.partitionMetadataResponse, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.SendReceipt -> 
                    XCommandSendReceipt (command.SendReceipt, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Message -> 
                    XCommandMessage (command.Message, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.LookupResponse -> 
                    XCommandLookupTopic (command.lookupTopicResponse, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Ping -> 
                    XCommandPing (command.Ping, buffer.GetPosition(int64 frameLength))
                | _ -> 
                    InvalidCommand (Exception("Unknown command type"))
            with
            | ex ->
                InvalidCommand ex
        else    
            IncompleteCommand
    else
        IncompleteCommand

let private readSocket (connection: Connection) (tsc: TaskCompletionSource<Connection>) =
    task {
        let (conn, _) = connection
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
                    tsc.SetResult(connection)
                    reader.AdvanceTo(consumed)
                | XCommandPartitionedTopicMetadataResponse (cmd, consumed) ->
                    let requestId = %cmd.RequestId
                    let tsc = requests.[requestId]
                    tsc.SetResult(PartitionedTopicMetadata { Partitions = cmd.Partitions })  
                    requests.TryRemove(requestId) |> ignore
                    reader.AdvanceTo(consumed)
                | XCommandSendReceipt (cmd, consumed) ->
                    let producerMb = producers.[%cmd.ProducerId]
                    producerMb.Post(SendReceipt cmd)   
                    reader.AdvanceTo(consumed)                
                | XCommandPing (cmd, consumed) ->
                    sendMb.Post(SocketMessageWithoutReply (connection, Commands.newPong()))
                    reader.AdvanceTo(consumed)         
                | XCommandMessage (cmd, consumed) ->
                    let consumerEvent = consumers.[%cmd.ConsumerId]
                    // TODO handle real messages
                    consumerEvent.Post(AddMessage { MessageId = MessageId.FromMessageIdData(cmd.MessageId); Payload = [||] })        
                    reader.AdvanceTo(consumed)
                | XCommandLookupTopic (cmd, consumed) ->
                    let requestId = %cmd.RequestId
                    let tsc = requests.[requestId]
                    let result = LookupTopicResult { BrokerServiceUrl = cmd.brokerServiceUrl; Proxy = cmd.ProxyThroughServiceUrl }
                    tsc.SetResult(result)  
                    requests.TryRemove(requestId) |> ignore
                    reader.AdvanceTo(consumed)
                | IncompleteCommand ->
                    reader.AdvanceTo(buffer.Start, buffer.End)
                | InvalidCommand ex ->
                    raise ex
    }



let private connect (address: EndPoint) =
    Log.Logger.LogInformation("Connecting to {0}", address)
    task {
        let! socketConnection = SocketConnection.ConnectAsync(address)
        let writerStream = StreamConnection.GetWriter(socketConnection.Output)
        let connection = (socketConnection, writerStream)
        let initialConnectionTsc = TaskCompletionSource<Connection>()
        let listener = Task.Run(fun() -> (readSocket connection initialConnectionTsc).Wait())
        let connectPayload = 
            Commands.newConnect clientVersion protocolVersion
        do! sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply ((connection, connectPayload), replyChannel))
        return! initialConnectionTsc.Task
    }    

let getConnection (broker: Broker) =   
    connections.GetOrAdd(broker.PhysicalAddress, fun(address) -> 
        lazy connect address).Value
               
let registerProducer (broker: Broker) (producerId: ProducerId) (producerMb: MailboxProcessor<ProducerMessage>) =
    let connection = getConnection broker
    producers.TryAdd(producerId, producerMb) |> ignore
    Log.Logger.LogInformation("Producer registered")
    connection

let registerConsumer (broker: Broker) (consumerId: ConsumerId) (consumerMb: MailboxProcessor<ConsumerMessage>) =
    let connection = getConnection broker
    consumers.TryAdd(consumerId, consumerMb) |> ignore
    Log.Logger.LogInformation("Consumer registered")
    connection

let send payload = 
    sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply(payload, replyChannel))

let sendAndWaitForReply reqId payload = 
    task {
        do! sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply(payload, replyChannel))
        let tsc = TaskCompletionSource()
        if requests.TryAdd(reqId, tsc) |> not
        then tsc.SetException(Exception("Duplicate request"))       
        return! tsc.Task
    }