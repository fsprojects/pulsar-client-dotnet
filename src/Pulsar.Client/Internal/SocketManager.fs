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
open Pulsar.Client.Api
open System.IO.Pipelines
open CRC32

let clientVersion = "Pulsar.Client v" + Assembly.GetExecutingAssembly().GetName().Version.ToString()
let protocolVersion =
    ProtocolVersion.GetValues(typeof<ProtocolVersion>)
    :?> ProtocolVersion[]
    |> Array.last


type PulsarTypes =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult
    | ProducerSuccess of ProducerSuccess
    | TopicsOfNamespace of TopicsOfNamespace
    | Empty

let connections = ConcurrentDictionary<LogicalAddres, Lazy<Task<Connection>>>()
let requests = ConcurrentDictionary<RequestId, TaskCompletionSource<PulsarTypes>>()
let consumers = ConcurrentDictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
let producers = ConcurrentDictionary<ProducerId, MailboxProcessor<ProducerMessage>>()


type PulsarCommands =
    | XCommandConnected of CommandConnected * SequencePosition
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse * SequencePosition
    | XCommandSendReceipt of CommandSendReceipt * SequencePosition
    | XCommandMessage of (CommandMessage * MessageMetadata * byte[] ) * SequencePosition
    | XCommandPing of CommandPing * SequencePosition
    | XCommandLookupTopicResponse of CommandLookupTopicResponse * SequencePosition
    | XCommandProducerSuccess of CommandProducerSuccess * SequencePosition
    | XCommandSuccess of CommandSuccess * SequencePosition
    | XCommandSendError of CommandSendError * SequencePosition
    | XCommandGetTopicsOfNamespaceResponse of CommandGetTopicsOfNamespaceResponse * SequencePosition
    | XCommandCloseProducer of CommandCloseProducer * SequencePosition
    | XCommandCloseConsumer of CommandCloseConsumer * SequencePosition
    | IncompleteCommand
    | InvalidCommand of Exception


type Payload = Connection*SerializedPayload

type SocketMessage =
    | SocketMessageWithReply of Payload * AsyncReplyChannel<unit>
    | SocketMessageWithoutReply of Payload

let sendSerializedPayload ((connection, serializedPayload): Payload ) =
    task {
        let (conn, streamWriter) = connection
        do! streamWriter |> serializedPayload

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
    if (array.Length >= 8)
    then
        use stream =  new MemoryStream(array)
        use reader = new BinaryReader(stream)

        let totalength = reader.ReadInt32() |> int32FromBigEndian
        let frameLength = totalength + 4

        if (frameLength <= array.Length)
        then
            try
                let command = Serializer.DeserializeWithLengthPrefix<BaseCommand>(stream, PrefixStyle.Fixed32BigEndian)
                Log.Logger.LogDebug("Got message of type {0}", command.``type``)
                match command.``type`` with
                | BaseCommand.Type.Connected ->
                    XCommandConnected (command.Connected, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.PartitionedMetadataResponse ->
                    XCommandPartitionedTopicMetadataResponse (command.partitionMetadataResponse, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.SendReceipt ->
                    XCommandSendReceipt (command.SendReceipt, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Message ->
                    reader.ReadInt16() |> int16FromBigEndian |> invalidArgIf ((<>) MagicNumber) "Invalid magicNumber" |> ignore
                    let messageCheckSum  = reader.ReadInt32() |> int32FromBigEndian
                    let metadataPointer = stream.Position
                    let metatada = Serializer.DeserializeWithLengthPrefix<MessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
                    let payloadPointer = stream.Position
                    let metadataLength = payloadPointer - metadataPointer |> int
                    let payloadLength = frameLength - (int payloadPointer)
                    let payload = reader.ReadBytes(payloadLength)
                    stream.Seek(metadataPointer, SeekOrigin.Begin) |> ignore
                    CRC32C.Get(0u, stream, metadataLength + payloadLength) |> int32 |> invalidArgIf ((<>) messageCheckSum) "Invalid checksum" |> ignore
                    XCommandMessage ((command.Message, metatada, payload), buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.LookupResponse ->
                    XCommandLookupTopicResponse (command.lookupTopicResponse, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Ping ->
                    XCommandPing (command.Ping, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.ProducerSuccess ->
                    XCommandProducerSuccess (command.ProducerSuccess, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.Success ->
                    XCommandSuccess (command.Success, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.SendError ->
                    XCommandSendError (command.SendError, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.CloseProducer ->
                    XCommandCloseProducer (command.CloseProducer, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.GetTopicsOfNamespaceResponse ->
                    XCommandGetTopicsOfNamespaceResponse (command.getTopicsOfNamespaceResponse, buffer.GetPosition(int64 frameLength))
                | _ as unknownCommandType ->
                    InvalidCommand (Exception(sprintf "Unknown command type: '%A'" unknownCommandType))
            with
            | ex ->
                InvalidCommand ex
        else
            IncompleteCommand
    else
        IncompleteCommand


let handleRespone requestId result (reader: PipeReader) consumed = 
    let tsc = requests.[requestId]    
    tsc.SetResult(result)
    requests.TryRemove(requestId) |> ignore
    reader.AdvanceTo(consumed)

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
                // TODO: handle closed socket
                continueLooping <- false
            else
                match tryParse buffer with
                | XCommandConnected (cmd, consumed) ->
                    //TODO check server protocol version
                    tsc.SetResult(connection)
                    reader.AdvanceTo(consumed)
                | XCommandPartitionedTopicMetadataResponse (cmd, consumed) ->
                    let result = PartitionedTopicMetadata { Partitions = cmd.Partitions }
                    handleRespone %cmd.RequestId result reader consumed
                | XCommandSendReceipt (cmd, consumed) ->
                    let producerMb = producers.[%cmd.ProducerId]
                    producerMb.Post(SendReceipt cmd)
                    reader.AdvanceTo(consumed)
                | XCommandSendError (cmd, consumed) ->
                    let producerMb = producers.[%cmd.ProducerId]
                    producerMb.Post(SendError cmd)
                    reader.AdvanceTo(consumed)
                | XCommandPing (cmd, consumed) ->
                    sendMb.Post(SocketMessageWithoutReply (connection, Commands.newPong()))
                    reader.AdvanceTo(consumed)
                | XCommandMessage ((cmd, metadata, payload), consumed) ->
                    let consumerMb = consumers.[%cmd.ConsumerId]
                    consumerMb.Post(MessageRecieved { MessageId = MessageId.FromMessageIdData(cmd.MessageId); Payload = payload })
                    reader.AdvanceTo(consumed)
                | XCommandLookupTopicResponse (cmd, consumed) ->
                    let result = LookupTopicResult { BrokerServiceUrl = cmd.brokerServiceUrl; Proxy = cmd.ProxyThroughServiceUrl }
                    handleRespone %cmd.RequestId result reader consumed
                | XCommandProducerSuccess (cmd, consumed) ->
                    let result = ProducerSuccess { GeneratedProducerName = cmd.ProducerName }
                    handleRespone %cmd.RequestId result reader consumed
                | XCommandSuccess (cmd, consumed) ->
                    handleRespone %cmd.RequestId Empty reader consumed
                | XCommandCloseProducer (cmd, consumed) ->
                    let producerMb = producers.[%cmd.ProducerId]
                    producerMb.Post(ProducerClosed producerMb)
                    reader.AdvanceTo(consumed)
                | XCommandCloseConsumer (cmd, consumed) ->
                    let consumerMb = consumers.[%cmd.ConsumerId]
                    consumerMb.Post(ConsumerClosed consumerMb)
                    reader.AdvanceTo(consumed)
                | XCommandGetTopicsOfNamespaceResponse (cmd, consumed) ->
                    let result = TopicsOfNamespace { Topics = List.ofSeq cmd.Topics }
                    handleRespone %cmd.RequestId result reader consumed
                | IncompleteCommand ->
                    reader.AdvanceTo(buffer.Start, buffer.End)
                | InvalidCommand ex ->
                    raise ex
    }

let private connect (broker: Broker) =
    Log.Logger.LogInformation("Connecting to {0}", broker)
    task {
        let (PhysicalAddress physicalAddress) = broker.PhysicalAddress
        let (LogicalAddres logicalAddres) = broker.LogicalAddress
        let! socketConnection = SocketConnection.ConnectAsync(physicalAddress)
        let writerStream = StreamConnection.GetWriter(socketConnection.Output)
        let connection = (socketConnection, writerStream)
        let initialConnectionTsc = TaskCompletionSource<Connection>()
        let listener = Task.Run(fun() -> (readSocket connection initialConnectionTsc).Wait())
        let proxyToBroker = if physicalAddress = logicalAddres then None else Some logicalAddres
        let connectPayload =
            Commands.newConnect clientVersion protocolVersion proxyToBroker
        do! sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply ((connection, connectPayload), replyChannel))
        return! initialConnectionTsc.Task
    }

let getConnection (broker: Broker) =
    connections.GetOrAdd(broker.LogicalAddress, fun(address) ->
        lazy connect broker).Value

let getBrokerlessConnection (address: DnsEndPoint) =
    getConnection { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress address }

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

let registerProducer (broker: Broker) (producerConfig: ProducerConfiguration) (producerId: ProducerId) (producerMb: MailboxProcessor<ProducerMessage>) =
    task {
        let! connection = getConnection broker
        producers.TryAdd(producerId, producerMb) |> ignore
        Log.Logger.LogInformation("Connection established for producer")
        let requestId = Generators.getNextRequestId()
        let payload =
            Commands.newProducer producerConfig.Topic producerConfig.ProducerName producerId requestId
        let! result = sendAndWaitForReply requestId (connection, payload)
        match result with
        | ProducerSuccess success ->
            Log.Logger.LogInformation("Producer registered with name {0}", success.GeneratedProducerName)
            return connection
        | _ ->
            return failwith "Incorrect return type"
    }

let registerConsumer (broker: Broker) (consumerConfig: ConsumerConfiguration) (consumerId: ConsumerId) (consumerMb: MailboxProcessor<ConsumerMessage>) =
    task {
        let! connection = getConnection broker
        consumers.TryAdd(consumerId, consumerMb) |> ignore
        Log.Logger.LogInformation("Connection established for consumer")
        let requestId = Generators.getNextRequestId()
        let payload =
            Commands.newSubscribe consumerConfig.Topic consumerConfig.SubscriptionName consumerId requestId consumerConfig.ConsumerName consumerConfig.SubscriptionType
        let! result = sendAndWaitForReply requestId (connection, payload)
        match result with
        | Empty ->
            Log.Logger.LogInformation("Consumer registered")
            let flowCommand =
                Commands.newFlow consumerId 1000u
            do! send (connection, flowCommand)
            Log.Logger.LogInformation("Consumer initial flow sent")
            return connection
        | _ ->
            return failwith "Incorrect return type"
    }