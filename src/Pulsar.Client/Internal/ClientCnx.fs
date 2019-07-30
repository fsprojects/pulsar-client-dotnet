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
open Pulsar.Client.Api

type CnxOperation =
    | AddProducer of ProducerId * MailboxProcessor<ProducerMessage>
    | AddConsumer of ConsumerId * MailboxProcessor<ConsumerMessage>
    | RemoveConsumer of ConsumerId
    | RemoveProducer of ProducerId
    | ChannelInactive

type PulsarCommand =
    | XCommandConnected of CommandConnected
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse
    | XCommandSendReceipt of CommandSendReceipt
    | XCommandMessage of (CommandMessage * MessageMetadata * byte[] )
    | XCommandPing of CommandPing
    | XCommandLookupTopicResponse of CommandLookupTopicResponse
    | XCommandProducerSuccess of CommandProducerSuccess
    | XCommandSuccess of CommandSuccess
    | XCommandSendError of CommandSendError
    | XCommandGetTopicsOfNamespaceResponse of CommandGetTopicsOfNamespaceResponse
    | XCommandCloseProducer of CommandCloseProducer
    | XCommandCloseConsumer of CommandCloseConsumer
    | XCommandReachedEndOfTopic of CommandReachedEndOfTopic
    | XCommandError of CommandError

type CommandParseError =
    | IncompleteCommand
    | UnknownCommandType of BaseCommand.Type

type SocketMessage =
    | SocketMessageWithReply of Payload * AsyncReplyChannel<unit>
    | SocketMessageWithoutReply of Payload
    | SocketRequestMessageWithReply of RequestId * Payload * AsyncReplyChannel<Task<PulsarTypes>>
    | Stop

type ClientCnx (broker: Broker,
                connection: Connection,
                initialConnectionTsc: TaskCompletionSource<ClientCnx>,
                unregisterClientCnx: Broker -> unit) as this =

    let consumers = Dictionary<ConsumerId, MailboxProcessor<ConsumerMessage>>()
    let producers = Dictionary<ProducerId, MailboxProcessor<ProducerMessage>>()
    let requests = Dictionary<RequestId, TaskCompletionSource<PulsarTypes>>()
    let clientCnxId = Generators.getNextClientCnxId()
    let prefix = sprintf "clientCnx(%i, %A)" clientCnxId broker.LogicalAddress

    let operationsMb = MailboxProcessor<CnxOperation>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | AddProducer (producerId, mb) ->
                    let z = prefix
                    Log.Logger.LogDebug("{0} adding producer {1}", prefix, producerId)
                    producers.Add(producerId, mb)
                    return! loop()
                | AddConsumer (consumerId, mb) ->
                    Log.Logger.LogDebug("{0} adding consumer {1}", prefix, consumerId)
                    consumers.Add(consumerId, mb)
                    return! loop()
                | RemoveConsumer consumerId ->
                    Log.Logger.LogDebug("{0} removing consumer {1}", prefix, consumerId)
                    consumers.Remove(consumerId) |> ignore
                    return! loop()
                | RemoveProducer producerId ->
                    Log.Logger.LogDebug("{0} removing producer {1}", prefix, producerId)
                    producers.Remove(producerId) |> ignore
                    return! loop()
                | ChannelInactive ->
                    Log.Logger.LogDebug("{0} ChannelInactive", prefix)
                    unregisterClientCnx(broker)
                    this.SendMb.Post(Stop)
                    consumers |> Seq.iter(fun kv ->
                        kv.Value.Post(ConsumerMessage.ConnectionClosed))
                    producers |> Seq.iter(fun kv ->
                        kv.Value.Post(ProducerMessage.ConnectionClosed))
            }
        loop ()
    )

    let sendSerializedPayload (serializedPayload: Payload ) =
        task {
            let (conn, streamWriter) = connection
            do! streamWriter |> serializedPayload

            let connected = conn.Socket.Connected
            if (not connected) then
                Log.Logger.LogWarning("{0} socket was disconnected on writing", prefix)
                operationsMb.Post(ChannelInactive)
            return connected
        }

    let sendMb = MailboxProcessor<SocketMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | SocketMessageWithReply (payload, replyChannel) ->
                    let! connected = sendSerializedPayload payload |> Async.AwaitTask
                    replyChannel.Reply()
                    if connected then
                        return! loop ()
                | SocketMessageWithoutReply payload ->
                    let! connected = sendSerializedPayload payload |> Async.AwaitTask
                    if connected then
                        return! loop ()
                | SocketRequestMessageWithReply (reqId, payload, replyChannel) ->
                    let! connected = sendSerializedPayload payload |> Async.AwaitTask
                    let tsc = TaskCompletionSource()
                    requests.Add(reqId, tsc)
                    replyChannel.Reply(tsc.Task)
                    if connected then
                        return! loop ()
                    else
                        tsc.SetException(Exception("Disconnected"))
                | Stop -> ()
            }
        loop ()
    )

    let wrapCommand readMessage (command: BaseCommand) =
        match command.``type`` with
        | BaseCommand.Type.Connected ->
            Ok (XCommandConnected command.Connected)
        | BaseCommand.Type.PartitionedMetadataResponse ->
            Ok (XCommandPartitionedTopicMetadataResponse command.partitionMetadataResponse)
        | BaseCommand.Type.SendReceipt ->
            Ok (XCommandSendReceipt command.SendReceipt)
        | BaseCommand.Type.Message ->
           let metadata,payload = readMessage()
           Ok (XCommandMessage (command.Message, metadata, payload))
        | BaseCommand.Type.LookupResponse ->
            Ok (XCommandLookupTopicResponse command.lookupTopicResponse)
        | BaseCommand.Type.Ping ->
            Ok (XCommandPing command.Ping)
        | BaseCommand.Type.ProducerSuccess ->
            Ok (XCommandProducerSuccess command.ProducerSuccess)
        | BaseCommand.Type.Success ->
            Ok (XCommandSuccess command.Success)
        | BaseCommand.Type.SendError ->
            Ok (XCommandSendError command.SendError)
        | BaseCommand.Type.CloseProducer ->
            Ok (XCommandCloseProducer command.CloseProducer)
        | BaseCommand.Type.CloseConsumer ->
            Ok (XCommandCloseConsumer command.CloseConsumer)
        | BaseCommand.Type.ReachedEndOfTopic ->
            Ok (XCommandReachedEndOfTopic command.reachedEndOfTopic)
        | BaseCommand.Type.GetTopicsOfNamespaceResponse ->
            Ok (XCommandGetTopicsOfNamespaceResponse command.getTopicsOfNamespaceResponse)
        | BaseCommand.Type.Error ->
            Ok (XCommandError command.Error)
        | unknownType ->
            Result.Error (UnknownCommandType unknownType)

    let tryParse (buffer: ReadOnlySequence<byte>) =
        let array = buffer.ToArray()
        if (array.Length >= 8) then
            use stream =  new MemoryStream(array)
            use reader = new BinaryReader(stream)

            let totalength = reader.ReadInt32() |> int32FromBigEndian
            let frameLength = totalength + 4

            if (array.Length >= frameLength) then
                let readMessage() =
                    reader.ReadInt16() |> int16FromBigEndian |> invalidArgIf ((<>) MagicNumber) "Invalid magicNumber" |> ignore
                    let messageCheckSum  = reader.ReadInt32() |> int32FromBigEndian
                    let metadataPointer = stream.Position
                    let metadata = Serializer.DeserializeWithLengthPrefix<MessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
                    let payloadPointer = stream.Position
                    let metadataLength = payloadPointer - metadataPointer |> int
                    let payloadLength = frameLength - (int payloadPointer)
                    let payload = reader.ReadBytes(payloadLength)
                    stream.Seek(metadataPointer, SeekOrigin.Begin) |> ignore
                    CRC32C.Get(0u, stream, metadataLength + payloadLength) |> int32 |> invalidArgIf ((<>) messageCheckSum) "Invalid checksum" |> ignore
                    metadata, payload
                let command = Serializer.DeserializeWithLengthPrefix<BaseCommand>(stream, PrefixStyle.Fixed32BigEndian)
                Log.Logger.LogDebug("{0} Got message of type {1}", prefix, command.``type``)
                let consumed = int64 frameLength |> buffer.GetPosition
                wrapCommand readMessage command, consumed
            else
                Result.Error IncompleteCommand, SequencePosition()
        else
            Result.Error IncompleteCommand, SequencePosition()

    let handleRespone requestId result =
        let tsc = requests.[requestId]
        tsc.SetResult result
        requests.Remove requestId |> ignore

    let handleCommand xcmd =
        match xcmd with
        | XCommandConnected _ ->
            //TODO check server protocol version
            initialConnectionTsc.SetResult(this)
        | XCommandPartitionedTopicMetadataResponse cmd ->
            let result =
                if (cmd.ShouldSerializeError()) then
                    Log.Logger.LogError("{0} CommandPartitionedTopicMetadataResponse Error: {1}. Message: {2}", prefix, cmd.Error, cmd.Message)
                    Error
                else
                    PartitionedTopicMetadata { Partitions = cmd.Partitions }
            handleRespone %cmd.RequestId result
        | XCommandSendReceipt cmd ->
            let producerMb = producers.[%cmd.ProducerId]
            producerMb.Post(SendReceipt cmd)
        | XCommandSendError cmd ->
            let producerMb = producers.[%cmd.ProducerId]
            producerMb.Post(SendError cmd)
        | XCommandPing _ ->
            Commands.newPong() |> SocketMessageWithoutReply |> sendMb.Post
        | XCommandMessage (cmd, _, payload) ->
            let consumerMb = consumers.[%cmd.ConsumerId]
            consumerMb.Post(MessageReceived { MessageId = MessageId.FromMessageIdData(cmd.MessageId); Payload = payload })
        | XCommandLookupTopicResponse cmd ->
            let result =
                if (cmd.ShouldSerializeError()) then
                    Log.Logger.LogError("{0} CommandLookupTopicResponse Error: {1}. Message: {2}", prefix, cmd.Error, cmd.Message)
                    Error
                else
                    LookupTopicResult { BrokerServiceUrl = cmd.brokerServiceUrl; Proxy = cmd.ProxyThroughServiceUrl }
            handleRespone %cmd.RequestId result
        | XCommandProducerSuccess cmd ->
            let result = ProducerSuccess { GeneratedProducerName = cmd.ProducerName }
            handleRespone %cmd.RequestId result
        | XCommandSuccess cmd ->
            handleRespone %cmd.RequestId Empty
        | XCommandCloseProducer cmd ->
            let producerMb = producers.[%cmd.ProducerId]
            producers.Remove(%cmd.ProducerId) |> ignore
            producerMb.Post ProducerMessage.ConnectionClosed
        | XCommandCloseConsumer cmd ->
            let consumerMb = consumers.[%cmd.ConsumerId]
            consumers.Remove(%cmd.ConsumerId) |> ignore
            consumerMb.Post ConnectionClosed
        | XCommandReachedEndOfTopic cmd ->
            let consumerMb = consumers.[%cmd.ConsumerId]
            consumerMb.Post ReachedEndOfTheTopic
        | XCommandGetTopicsOfNamespaceResponse cmd ->
            let result = TopicsOfNamespace { Topics = List.ofSeq cmd.Topics }
            handleRespone %cmd.RequestId result
        | XCommandError cmd ->
            Log.Logger.LogError("{0} CommandError Error: {1}. Message: {2}", prefix, cmd.Error, cmd.Message)
            let result = Error
            handleRespone %cmd.RequestId result

    let readSocket () =
        task {
            Log.Logger.LogDebug("{0} Started read socket", prefix)
            let (conn, _) = connection
            let mutable continueLooping = true
            let reader = conn.Input
            while continueLooping do
                let! result = reader.ReadAsync()
                let buffer = result.Buffer
                if result.IsCompleted then
                    if initialConnectionTsc.TrySetException(Exception("Unable to initiate connection")) then
                        Log.Logger.LogWarning("{0} New connection was aborted", prefix)
                    Log.Logger.LogWarning("{0} Socket was disconnected while reading", prefix)
                    operationsMb.Post(ChannelInactive)
                    continueLooping <- false
                else
                    match tryParse buffer with
                    | Result.Ok (xcmd), consumed ->
                        handleCommand xcmd
                        reader.AdvanceTo consumed
                    | Result.Error IncompleteCommand, _ ->
                        Log.Logger.LogDebug("{0} IncompleteCommand received", prefix)
                        reader.AdvanceTo(buffer.Start, buffer.End)
                    | Result.Error (UnknownCommandType unknownType), _ ->
                        failwithf "Unknown command type %A" unknownType
            Log.Logger.LogDebug("{0} Finished read socket", prefix)
        }

    do Task.Run(fun () -> readSocket().Wait()) |> ignore

    member private __.SendMb with get(): MailboxProcessor<SocketMessage> = sendMb

    member __.Send payload =
        sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply(payload, replyChannel))

    member __.SendAndWaitForReply reqId payload =
        task {
            let! task = sendMb.PostAndAsyncReply(fun replyChannel -> SocketRequestMessageWithReply(reqId, payload, replyChannel))
            return! task
        }

    member __.RemoveConsumer (consumerId: ConsumerId) =
        operationsMb.Post(RemoveConsumer(consumerId))

    member __.RemoveProducer (consumerId: ProducerId) =
        operationsMb.Post(RemoveProducer(consumerId))

    member __.AddProducer (producerId: ProducerId) (producerMb: MailboxProcessor<ProducerMessage>) =
        operationsMb.Post(AddProducer (producerId, producerMb))

    member __.AddConsumer (consumerId: ConsumerId) (consumerMb: MailboxProcessor<ConsumerMessage>) =
        operationsMb.Post(AddConsumer (consumerId, consumerMb))

    member __.Close() =
        let (conn, writeStream) = connection
        conn.Dispose()
        writeStream.Dispose()
