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
open System.Collections.Generic

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
    | Error
    | Empty

let connections = ConcurrentDictionary<LogicalAddress, Lazy<Task<Connection>>>()
let requests = ConcurrentDictionary<RequestId, TaskCompletionSource<PulsarTypes>>()
let consumers = Dictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
let consumersByAddress = Dictionary<LogicalAddress, HashSet<ConsumerId>>()
let producers = Dictionary<ProducerId, MailboxProcessor<ProducerMessage>>()
let producersByAddress = Dictionary<LogicalAddress, HashSet<ProducerId>>()


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
    | XCommandReachedEndOfTopic of CommandReachedEndOfTopic * SequencePosition
    | IncompleteCommand
    | InvalidCommand of Exception


type Payload = Connection * SerializedPayload

type SocketMessage =
    | SocketMessageWithReply of Payload * AsyncReplyChannel<unit>
    | SocketMessageWithoutReply of Payload

type OperationsMessage =
    | AddConsumer of ConsumerId * MailboxProcessor<ConsumerMessage> * LogicalAddress
    | ReconnectConsumer of ConsumerId * LogicalAddress
    | AddProducer of ProducerId * MailboxProcessor<ProducerMessage> * LogicalAddress
    | ReconnectProducer of ProducerId * LogicalAddress
    | SocketDisconnected of Connection * LogicalAddress

let private addToAddresses (dictionary: Dictionary<LogicalAddress, HashSet<'a>>) address value =
    match dictionary.TryGetValue address with
    | true, collection ->
        collection.Add(value) |> ignore
    | _ ->
        let collection = HashSet<'a>()
        collection.Add(value) |> ignore
        dictionary.Add(address, collection)


let operationsMb = MailboxProcessor<OperationsMessage>.Start(fun inbox ->
    let rec loop () =
        async {
            match! inbox.Receive() with
            | AddConsumer (consumerId, mb, logicalAddress) ->
                consumers.Add(consumerId, mb)
                addToAddresses consumersByAddress logicalAddress consumerId
            | ReconnectConsumer(consumerId, logicalAddress) ->
                addToAddresses consumersByAddress logicalAddress consumerId
            | AddProducer (producerId, mb, logicalAddress) ->
                producers.Add(producerId, mb)
                addToAddresses producersByAddress logicalAddress producerId
            | ReconnectProducer(producerId, logicalAddress) ->
                addToAddresses producersByAddress logicalAddress producerId
            | SocketDisconnected (connection, logicalAddress) ->
                connections.TryRemove(logicalAddress) |> ignore
                match consumersByAddress.TryGetValue(logicalAddress) with
                | true, consumerIds ->
                    consumerIds
                    |> Seq.iter (fun consumerId ->
                        match consumers.TryGetValue(consumerId) with
                        | true, mb ->
                            mb.Post(ConsumerMessage.Disconnected (connection, mb))
                        | _  -> ())
                    consumersByAddress.Remove(logicalAddress) |> ignore
                | _ -> ()
                match producersByAddress.TryGetValue(logicalAddress) with
                | true, consumerIds ->
                    consumerIds
                    |> Seq.iter (fun producerId ->
                        match producers.TryGetValue(producerId) with
                        | true, mb -> mb.Post(ProducerMessage.Disconnected (connection, mb))
                        | _  -> ())
                    producersByAddress.Remove(logicalAddress) |> ignore
                | _ -> ()
            return! loop ()
        }
    loop ()
)

let private sendSerializedPayload ((connection, serializedPayload): Payload ) =
    task {
        let (conn, streamWriter, broker) = connection
        do! streamWriter |> serializedPayload

        if (not conn.Socket.Connected)
        then
            Log.Logger.LogWarning("Socket was disconnected on writing")
            operationsMb.Post(SocketDisconnected(connection, broker.LogicalAddress))
    }

let sendMb = MailboxProcessor<SocketMessage>.Start(fun inbox ->
    let rec loop () =
        async {
            match! inbox.Receive() with
            | SocketMessageWithReply (payload, replyChannel) ->
                do! sendSerializedPayload payload |> Async.AwaitTask
                replyChannel.Reply()
            | SocketMessageWithoutReply payload ->
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
                | BaseCommand.Type.CloseConsumer ->
                    XCommandCloseConsumer (command.CloseConsumer, buffer.GetPosition(int64 frameLength))
                | BaseCommand.Type.ReachedEndOfTopic ->
                    XCommandReachedEndOfTopic (command.reachedEndOfTopic, buffer.GetPosition(int64 frameLength))
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

let private readSocket (connection: Connection) (tsc: TaskCompletionSource<Connection>) (logicalAddress: LogicalAddress)  =
    task {
        let (conn, _, _) = connection
        let mutable continueLooping = true
        let reader = conn.Input
        while continueLooping do
            let! result = reader.ReadAsync()
            let buffer = result.Buffer
            if result.IsCompleted
            then
                Log.Logger.LogWarning("Socket was disconnected while reading")
                operationsMb.Post(SocketDisconnected(connection, logicalAddress))
                continueLooping <- false
            else
                match tryParse buffer with
                | XCommandConnected (cmd, consumed) ->
                    //TODO check server protocol version
                    tsc.SetResult(connection)
                    reader.AdvanceTo(consumed)
                | XCommandPartitionedTopicMetadataResponse (cmd, consumed) ->
                    let result =
                        if (cmd.ShouldSerializeError())
                        then
                            Log.Logger.LogError("Error: {0}. Message: {1}", cmd.Error, cmd.Message)
                            Error
                        else
                            PartitionedTopicMetadata { Partitions = cmd.Partitions }
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
                    let result =
                        if (cmd.ShouldSerializeError())
                        then
                            Log.Logger.LogError("Error: {0}. Message: {1}", cmd.Error, cmd.Message)
                            Error
                        else
                            LookupTopicResult { BrokerServiceUrl = cmd.brokerServiceUrl; Proxy = cmd.ProxyThroughServiceUrl }
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
                | XCommandReachedEndOfTopic (cmd, consumed) ->
                    let consumerMb = consumers.[%cmd.ConsumerId]
                    consumerMb.Post ReachedEndOfTheTopic
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
        let (LogicalAddress logicalAddres) = broker.LogicalAddress
        let! socketConnection = SocketConnection.ConnectAsync(physicalAddress)
        let writerStream = StreamConnection.GetWriter(socketConnection.Output)
        let connection = (socketConnection, writerStream, broker)
        let initialConnectionTsc = TaskCompletionSource<Connection>()
        let listener = Task.Run(fun() -> (readSocket connection initialConnectionTsc broker.LogicalAddress).Wait())
        let proxyToBroker = if physicalAddress = logicalAddres then None else Some logicalAddres
        let connectPayload =
            Commands.newConnect clientVersion protocolVersion proxyToBroker
        do! sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply ((connection, connectPayload), replyChannel))
        return! initialConnectionTsc.Task
    }

let private getConnection (broker: Broker) =
    connections.GetOrAdd(broker.LogicalAddress, fun(address) ->
        lazy connect broker).Value

let reconnectProducer (broker: Broker) (producerId: ProducerId) =
    task {
        let! connection = getConnection broker
        operationsMb.Post(ReconnectProducer (producerId, broker.LogicalAddress))
        return connection
    }

let reconnectConsumer (broker: Broker) (consumerId: ConsumerId) =
    task {
        let! connection = getConnection broker
        operationsMb.Post(ReconnectConsumer (consumerId, broker.LogicalAddress))
        return connection
    }

let getBrokerlessConnection (address: DnsEndPoint) =
    getConnection { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }

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
        operationsMb.Post(AddProducer (producerId, producerMb, broker.LogicalAddress))
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
        operationsMb.Post(AddConsumer (consumerId, consumerMb, broker.LogicalAddress))
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