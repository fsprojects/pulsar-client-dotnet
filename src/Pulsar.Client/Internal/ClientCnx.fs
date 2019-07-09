namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System.Collections.Generic
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging
open System.Threading.Tasks
open pulsar.proto
open System
open System.IO.Pipelines
open FSharp.UMX
open System.Buffers
open System.IO
open ProtoBuf
open CRC32
open System.Threading
open Pulsar.Client.Api

type CnxOperation =
    | AddProducer of ProducerId * MailboxProcessor<ProducerMessage>
    | AddConsumer of ConsumerId * MailboxProcessor<ConsumerMessage>
    | Disconnected

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
    | XCommandError of CommandError * SequencePosition
    | IncompleteCommand
    | InvalidCommand of Exception

// type Payload = Connection * SerializedPayload

type PulsarTypes =
    | PartitionedTopicMetadata of PartitionedTopicMetadata
    | LookupTopicResult of LookupTopicResult
    | ProducerSuccess of ProducerSuccess
    | TopicsOfNamespace of TopicsOfNamespace
    | Error
    | Empty


type SocketMessage =
    | SocketMessageWithReply of SerializedPayload * AsyncReplyChannel<unit>
    | SocketMessageWithoutReply of SerializedPayload
    | SocketRequestMessageWithReply of RequestId * SerializedPayload * AsyncReplyChannel<Task<PulsarTypes>>


type ClientCnx (broker: Broker,
                connection: Connection,
                initialConnectionTsc: TaskCompletionSource<ClientCnx>,
                removeConnection: Broker -> unit) as this =
    let consumers = Dictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
    let producers = Dictionary<ProducerId, MailboxProcessor<ProducerMessage>>()
    let requests = Dictionary<RequestId, TaskCompletionSource<PulsarTypes>>()
    let cts = new CancellationTokenSource()

    let operationsMb = MailboxProcessor<CnxOperation>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | AddProducer (producerId, mb) ->
                    producers.Add(producerId, mb)
                    return! loop()
                | AddConsumer (consumerId, mb) ->
                    consumers.Add(consumerId, mb)
                    return! loop()
                | Disconnected ->
                    cts.Cancel()
                    removeConnection(broker)
                    consumers |> Seq.iter(fun kv -> kv.Value.Post(ConsumerMessage.Disconnected))
                    producers |> Seq.iter(fun kv -> kv.Value.Post(ProducerMessage.Disconnected))
            }
        loop ()
    )

    let sendSerializedPayload (serializedPayload: SerializedPayload ) =
        task {
            let (conn, streamWriter) = connection
            do! streamWriter |> serializedPayload

            if (not conn.Socket.Connected)
            then
                Log.Logger.LogWarning("Socket was disconnected on writing {0}", broker.LogicalAddress)
                operationsMb.Post(Disconnected)
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
                | SocketRequestMessageWithReply (reqId, payload, replyChannel) ->
                    do! sendSerializedPayload payload |> Async.AwaitTask
                    let tsc = TaskCompletionSource()
                    requests.Add(reqId, tsc)
                    replyChannel.Reply(tsc.Task)
                return! loop ()
            }
        loop ()
    , cts.Token)

    let tryParse (buffer: ReadOnlySequence<byte>) readerId =
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
                    Log.Logger.LogDebug("[{0}] Got message of type {1}", readerId, command.``type``)
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
                    | BaseCommand.Type.Error ->
                        XCommandError (command.Error, buffer.GetPosition(int64 frameLength))
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
        requests.Remove(requestId) |> ignore
        reader.AdvanceTo(consumed)

    let readSocket () =
        task {
            let readerId = Generators.getNextSocketReaderId()
            Log.Logger.LogDebug("[{0}] Started read socket for {1}", readerId, broker)
            let (conn, _) = connection
            let mutable continueLooping = true
            let reader = conn.Input
            while continueLooping && (cts.IsCancellationRequested |> not) do
                let! result = reader.ReadAsync()
                let buffer = result.Buffer
                if result.IsCompleted
                then
                    if
                        initialConnectionTsc.TrySetException(Exception("Unable to initiate connection"))
                    then
                        Log.Logger.LogWarning("[{0}] New connection to {1} was aborted", readerId, broker)
                    Log.Logger.LogWarning("[{0}] Socket was disconnected while reading {1}", readerId, broker)
                    operationsMb.Post(Disconnected)
                    continueLooping <- false
                else
                    match tryParse buffer readerId with
                    | XCommandConnected (cmd, consumed) ->
                        //TODO check server protocol version
                        initialConnectionTsc.SetResult(this)
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
                        sendMb.Post(SocketMessageWithoutReply (Commands.newPong()))
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
                        producers.Remove(%cmd.ProducerId) |> ignore
                        producerMb.Post(ProducerMessage.Disconnected)
                        reader.AdvanceTo(consumed)
                    | XCommandCloseConsumer (cmd, consumed) ->
                        let consumerMb = consumers.[%cmd.ConsumerId]
                        consumers.Remove(%cmd.ConsumerId) |> ignore
                        consumerMb.Post(ConsumerMessage.Disconnected)
                        reader.AdvanceTo(consumed)
                    | XCommandReachedEndOfTopic (cmd, consumed) ->
                        let consumerMb = consumers.[%cmd.ConsumerId]
                        consumerMb.Post ReachedEndOfTheTopic
                        reader.AdvanceTo(consumed)
                    | XCommandGetTopicsOfNamespaceResponse (cmd, consumed) ->
                        let result = TopicsOfNamespace { Topics = List.ofSeq cmd.Topics }
                        handleRespone %cmd.RequestId result reader consumed
                    | XCommandError (cmd, consumed) ->
                        Log.Logger.LogError("Error: {0}. Message: {1}", cmd.Error, cmd.Message)
                        let result = Error
                        handleRespone %cmd.RequestId result reader consumed
                    | IncompleteCommand ->
                        reader.AdvanceTo(buffer.Start, buffer.End)
                    | InvalidCommand ex ->
                        raise ex
            Log.Logger.LogDebug("[{0}] Finished read socket for {1}", readerId, broker)
        }

    do Task.Run(fun () -> readSocket().Wait(), cts.Token) |> ignore

    member __.Send payload =
        sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply(payload, replyChannel))

    member __.SendAndWaitForReply reqId payload =
        task {
            let! task = sendMb.PostAndAsyncReply(fun replyChannel -> SocketRequestMessageWithReply(reqId, payload, replyChannel))
            return! task
        }

    member __.RegisterProducer (producerConfig: ProducerConfiguration) (producerId: ProducerId) (producerMb: MailboxProcessor<ProducerMessage>) =
        task {
            Log.Logger.LogInformation("Starting register producer {0}", producerId)
            operationsMb.Post(AddProducer (producerId, producerMb))
            let requestId = Generators.getNextRequestId()
            let payload =
                Commands.newProducer producerConfig.Topic producerConfig.ProducerName producerId requestId
            let! result = __.SendAndWaitForReply requestId payload
            match result with
            | ProducerSuccess success ->
                Log.Logger.LogInformation("Producer {0} registered with name {1}", producerId, success.GeneratedProducerName)
            | _ ->
                failwith "Incorrect return type"
        }

    member __.RegisterConsumer (consumerConfig: ConsumerConfiguration) (consumerId: ConsumerId) (consumerMb: MailboxProcessor<ConsumerMessage>) =
        task {
            Log.Logger.LogInformation("Starting register consumer {0}", consumerId)
            operationsMb.Post(AddConsumer (consumerId, consumerMb))
            let requestId = Generators.getNextRequestId()
            let payload =
                Commands.newSubscribe consumerConfig.Topic consumerConfig.SubscriptionName consumerId requestId consumerConfig.ConsumerName consumerConfig.SubscriptionType
            let! result =  __.SendAndWaitForReply requestId payload
            match result with
            | Empty ->
                Log.Logger.LogInformation("Consumer {0} registered", consumerId)
                let initialFlowCount = consumerConfig.ReceiverQueueSize |> uint32
                let flowCommand =
                    Commands.newFlow consumerId initialFlowCount
                do! __.Send flowCommand
                Log.Logger.LogInformation("Consumer initial flow sent {0}", initialFlowCount)
            | _ ->
                failwith "Incorrect return type"
        }

type ConnectionState =
    | NotConnected
    | Connected of ClientCnx