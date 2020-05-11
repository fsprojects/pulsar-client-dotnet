namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System.Collections.Generic
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging
open System.Threading.Tasks
open pulsar.proto
open System
open FSharp.UMX
open System.Buffers
open System.IO
open ProtoBuf
open System.Threading
open Pulsar.Client.Api

type internal ProducerOperations =
    {
        AckReceived: SendReceipt -> unit
        TopicTerminatedError: unit -> unit
        RecoverChecksumError: SequenceId -> unit
        ConnectionClosed: ClientCnx -> unit
    }    
and internal ConsumerOperations =
    {
        MessageReceived: RawMessage * ClientCnx -> unit
        ReachedEndOfTheTopic: unit -> unit
        ActiveConsumerChanged: bool -> unit
        ConnectionClosed: ClientCnx -> unit
    }
    
and internal RequestsOperation =
    | AddRequest of RequestId * TaskCompletionSource<PulsarResponseType>
    | CompleteRequest of RequestId * PulsarResponseType
    | FailRequest of RequestId * exn
    | FailAllRequestsAndStop
    
and internal CnxOperation =
    | AddProducer of ProducerId * ProducerOperations
    | AddConsumer of ConsumerId * ConsumerOperations
    | RemoveConsumer of ConsumerId
    | RemoveProducer of ProducerId
    | ChannelInactive
    | Stop
    
and internal PulsarCommand =
    | XCommandConnected of CommandConnected
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse
    | XCommandSendReceipt of CommandSendReceipt
    | XCommandMessage of (CommandMessage * MessageMetadata * byte[] * bool )
    | XCommandPing of CommandPing
    | XCommandLookupTopicResponse of CommandLookupTopicResponse
    | XCommandProducerSuccess of CommandProducerSuccess
    | XCommandSuccess of CommandSuccess
    | XCommandSendError of CommandSendError
    | XCommandGetTopicsOfNamespaceResponse of CommandGetTopicsOfNamespaceResponse
    | XCommandGetLastMessageIdResponse of CommandGetLastMessageIdResponse
    | XCommandCloseProducer of CommandCloseProducer
    | XCommandCloseConsumer of CommandCloseConsumer
    | XCommandReachedEndOfTopic of CommandReachedEndOfTopic
    | XCommandActiveConsumerChange of CommandActiveConsumerChange
    | XCommandGetSchemaResponse of CommandGetSchemaResponse
    | XCommandError of CommandError

and internal CommandParseError =
    | IncompleteCommand
    | CorruptedCommand of exn
    | UnknownCommandType of BaseCommand.Type

and internal SocketMessage =
    | SocketMessageWithReply of Payload * AsyncReplyChannel<bool>
    | SocketMessageWithoutReply of Payload
    | SocketRequestMessageWithReply of RequestId * Payload * AsyncReplyChannel<Task<PulsarResponseType>>
    | Stop

and internal ClientCnx (config: PulsarClientConfiguration,
                broker: Broker,
                connection: Connection,
                maxMessageSize: int,
                brokerless: bool,
                initialConnectionTsc: TaskCompletionSource<ClientCnx>,
                unregisterClientCnx: Broker -> unit) as this =

    let consumers = Dictionary<ConsumerId, ConsumerOperations>()
    let producers = Dictionary<ProducerId, ProducerOperations>()
    let requests = Dictionary<RequestId, TaskCompletionSource<PulsarResponseType>>()
    let clientCnxId = Generators.getNextClientCnxId()
    let prefix = sprintf "clientCnx(%i, %A)" %clientCnxId broker.LogicalAddress
    let maxNumberOfRejectedRequestPerConnection = config.MaxNumberOfRejectedRequestPerConnection
    let rejectedRequestResetTimeSec = 60
    let mutable numberOfRejectedRequests = 0
    let mutable isActive = true
    
    let requestsMb = MailboxProcessor<RequestsOperation>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | AddRequest (reqId, tsc) ->
                    Log.Logger.LogDebug("{0} add request {1}", prefix, reqId)
                    requests.Add(reqId, tsc)
                    return! loop()
                | CompleteRequest (reqId, result)  ->
                    Log.Logger.LogDebug("{0} complete request {1}", prefix, reqId)
                    match requests.TryGetValue(reqId) with
                    | true, tsc ->
                        tsc.SetResult result
                        requests.Remove reqId |> ignore
                    | _ ->
                        Log.Logger.LogWarning("{0} complete non-existent request {1}, ignoring", prefix, reqId)
                    return! loop()
                | FailRequest (reqId, ex)  ->
                    Log.Logger.LogDebug("{0} fail request {1}", prefix, reqId)
                    match requests.TryGetValue(reqId) with
                    | true, tsc ->
                        tsc.SetException ex
                        requests.Remove reqId |> ignore
                    | _ ->
                        Log.Logger.LogWarning("{0} fail non-existent request {1}, ignoring", prefix, reqId)
                    return! loop()
                | FailAllRequestsAndStop ->
                    Log.Logger.LogDebug("{0} fail requests and stop", prefix)
                    requests |> Seq.iter (fun kv ->
                        kv.Value.SetException(ConnectException("Disconnected.")))
                    requests.Clear()
            }
        loop ()
    )

    let tryStopMailboxes() =
        if consumers.Count = 0 && producers.Count = 0 then
            this.SendMb.Post(SocketMessage.Stop)
            this.OperationsMb.Post(CnxOperation.Stop)

    let operationsMb = MailboxProcessor<CnxOperation>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | AddProducer (producerId, producerOperation) ->
                    Log.Logger.LogDebug("{0} adding producer {1}", prefix, producerId)
                    producers.Add(producerId, producerOperation)
                    return! loop()
                | AddConsumer (consumerId, consumerOperation) ->
                    Log.Logger.LogDebug("{0} adding consumer {1}", prefix, consumerId)
                    consumers.Add(consumerId, consumerOperation)
                    return! loop()
                | RemoveConsumer consumerId ->
                    Log.Logger.LogDebug("{0} removing consumer {1}", prefix, consumerId)
                    consumers.Remove(consumerId) |> ignore
                    if isActive |> not then
                        tryStopMailboxes()
                    return! loop()
                | RemoveProducer producerId ->
                    Log.Logger.LogDebug("{0} removing producer {1}", prefix, producerId)
                    producers.Remove(producerId) |> ignore
                    if isActive |> not then
                        tryStopMailboxes()
                    return! loop()
                | ChannelInactive ->
                    if isActive then
                        Log.Logger.LogDebug("{0} ChannelInactive", prefix)
                        isActive <- false
                        unregisterClientCnx(broker)
                        requestsMb.Post(FailAllRequestsAndStop)
                        consumers |> Seq.iter(fun (KeyValue(_,consumerOperation)) ->
                            consumerOperation.ConnectionClosed(this))
                        producers |> Seq.iter(fun (KeyValue(_,producerOperation)) ->
                            producerOperation.ConnectionClosed(this))
                    return! loop()
                | CnxOperation.Stop ->
                    Log.Logger.LogDebug("{0} operationsMb stopped", prefix)
            }
        loop ()
    )

    let sendSerializedPayload (serializedPayload: Payload ) =
        task {
            try
                do! connection.Output |> serializedPayload
                return true
            with ex ->
                Log.Logger.LogWarning(ex, "{0} Socket was disconnected exceptionally on writing", prefix)
                operationsMb.Post(ChannelInactive)
                return false
        }

    let sendMb = MailboxProcessor<SocketMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | SocketMessageWithReply (payload, replyChannel) ->
                    let! connected = sendSerializedPayload payload |> Async.AwaitTask
                    replyChannel.Reply(connected)
                    return! loop ()
                | SocketMessageWithoutReply payload ->
                    let! _ = sendSerializedPayload payload |> Async.AwaitTask
                    return! loop ()
                | SocketRequestMessageWithReply (reqId, payload, replyChannel) ->
                    let tsc = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
                    requestsMb.Post(AddRequest(reqId, tsc))
                    let! _ = sendSerializedPayload payload |> Async.AwaitTask
                    replyChannel.Reply(tsc.Task)
                    return! loop ()
                | SocketMessage.Stop ->
                    Log.Logger.LogDebug("{0} sendMb stopped", prefix)
            }
        loop ()
    )

    let readMessage (reader: BinaryReader) (stream: MemoryStream) frameLength =
        reader.ReadInt16() |> int16FromBigEndian |> invalidArgIf ((<>) MagicNumber) "Invalid magicNumber" |> ignore
        let messageCheckSum  = reader.ReadInt32() |> int32FromBigEndian
        let metadataPointer = stream.Position
        let metadata = Serializer.DeserializeWithLengthPrefix<MessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
        let payloadPointer = stream.Position
        let metadataLength = payloadPointer - metadataPointer |> int
        let payloadLength = frameLength - (int payloadPointer)
        let payload = reader.ReadBytes(payloadLength)
        stream.Seek(metadataPointer, SeekOrigin.Begin) |> ignore
        let calculatedCheckSum = CRC32C.Get(0u, stream, metadataLength + payloadLength) |> int32
        if (messageCheckSum <> calculatedCheckSum) then
            Log.Logger.LogError("{0} Invalid checksum. Received: {1} Calculated: {2}", prefix, messageCheckSum, calculatedCheckSum)
        (metadata, payload, messageCheckSum = calculatedCheckSum)
    
    let readCommand (command: BaseCommand) reader stream frameLength =
        match command.``type`` with
        | BaseCommand.Type.Connected ->
            Ok (XCommandConnected command.Connected)
        | BaseCommand.Type.PartitionedMetadataResponse ->
            Ok (XCommandPartitionedTopicMetadataResponse command.partitionMetadataResponse)
        | BaseCommand.Type.SendReceipt ->
            Ok (XCommandSendReceipt command.SendReceipt)
        | BaseCommand.Type.Message ->
            let (metadata,payload,checksumValid) = readMessage reader stream frameLength
            Ok (XCommandMessage (command.Message, metadata, payload, checksumValid))
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
        | BaseCommand.Type.GetLastMessageIdResponse ->
            Ok (XCommandGetLastMessageIdResponse command.getLastMessageIdResponse)
        | BaseCommand.Type.ActiveConsumerChange ->
            Ok (XCommandActiveConsumerChange command.ActiveConsumerChange)
        | BaseCommand.Type.GetSchemaResponse ->
            Ok (XCommandGetSchemaResponse command.getSchemaResponse)
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
                let command = Serializer.DeserializeWithLengthPrefix<BaseCommand>(stream, PrefixStyle.Fixed32BigEndian)
                Log.Logger.LogDebug("{0} Got message of type {1}", prefix, command.``type``)
                let consumed = int64 frameLength |> buffer.GetPosition
                try
                    let wrappedCommand = readCommand command reader stream frameLength
                    wrappedCommand, consumed
                with ex ->
                    Result.Error (CorruptedCommand ex), consumed
            else
                Result.Error IncompleteCommand, SequencePosition()
        else
            Result.Error IncompleteCommand, SequencePosition()
        

    let handleSuccess requestId result =
        requestsMb.Post(CompleteRequest(requestId, result))

    let getPulsarClientException error errorMsg =
        match error with
        | ServerError.AuthenticationError -> AuthenticationException errorMsg
        | ServerError.AuthorizationError -> AuthorizationException errorMsg
        | ServerError.ProducerBusy -> ProducerBusyException errorMsg
        | ServerError.ConsumerBusy -> ConsumerBusyException errorMsg
        | ServerError.MetadataError -> BrokerMetadataException errorMsg
        | ServerError.PersistenceError -> BrokerPersistenceException errorMsg
        | ServerError.ServiceNotReady -> LookupException errorMsg
        | ServerError.TooManyRequests -> TooManyRequestsException errorMsg
        | ServerError.ProducerBlockedQuotaExceededError -> ProducerBlockedQuotaExceededError errorMsg
        | ServerError.ProducerBlockedQuotaExceededException -> ProducerBlockedQuotaExceededException errorMsg
        | ServerError.TopicTerminatedError -> TopicTerminatedException errorMsg
        | ServerError.IncompatibleSchema -> IncompatibleSchemaException errorMsg
        | ServerError.TopicNotFound -> TopicDoesNotExistException errorMsg
        | _ -> Exception errorMsg

    let handleError requestId error msg =
        let exc = getPulsarClientException error msg
        requestsMb.Post(FailRequest(requestId, exc))

    let checkServerError serverError errMsg =
        if (serverError = ServerError.ServiceNotReady) then
            Log.Logger.LogError("{0} Close connection because received internal-server error {1}", prefix, errMsg);
            this.Close()
        elif (serverError = ServerError.TooManyRequests) then
            let rejectedRequests = Interlocked.Increment(&numberOfRejectedRequests)
            if (rejectedRequests = 1) then
                // schedule timer
                asyncDelay (rejectedRequestResetTimeSec*1000) (fun() -> Interlocked.Exchange(&numberOfRejectedRequests, 0) |> ignore)
            elif (rejectedRequests >= maxNumberOfRejectedRequestPerConnection) then
                Log.Logger.LogError("{0} Close connection because received {1} rejected request in {2} seconds ", prefix,
                        rejectedRequests, rejectedRequestResetTimeSec);
                this.Close()

    let getOptionalSchemaVersion =
        Option.ofObj >> Option.map SchemaVersion
    
    let getMessageReceived (cmd: CommandMessage) (messageMetadata: MessageMetadata) payload checkSumValid =
        let mapCompressionType = function
            | pulsar.proto.CompressionType.None -> Pulsar.Client.Common.CompressionType.None
            | pulsar.proto.CompressionType.Lz4 -> Pulsar.Client.Common.CompressionType.LZ4
            | pulsar.proto.CompressionType.Zlib -> Pulsar.Client.Common.CompressionType.ZLib
            | pulsar.proto.CompressionType.Zstd -> Pulsar.Client.Common.CompressionType.ZStd
            | pulsar.proto.CompressionType.Snappy -> Pulsar.Client.Common.CompressionType.Snappy
            | _ -> Pulsar.Client.Common.CompressionType.None
        let medadata = {
            NumMessages = messageMetadata.NumMessagesInBatch
            HasNumMessagesInBatch = messageMetadata.ShouldSerializeNumMessagesInBatch()
            CompressionType = messageMetadata.Compression |> mapCompressionType
            UncompressedMessageSize = messageMetadata.UncompressedSize |> int32
            SchemaVersion = getOptionalSchemaVersion messageMetadata.SchemaVersion
        } 
            
        {
            MessageId = { LedgerId = %(int64 cmd.MessageId.ledgerId); EntryId = %(int64 cmd.MessageId.entryId); Type = Individual; Partition = -1; TopicName = %"" }
            RedeliveryCount = cmd.RedeliveryCount
            Metadata = medadata
            Payload = payload
            MessageKey = messageMetadata.PartitionKey
            IsKeyBase64Encoded = messageMetadata.PartitionKeyB64Encoded
            CheckSumValid = checkSumValid
            Properties =
                if messageMetadata.Properties.Count > 0 then
                    messageMetadata.Properties
                    |> Seq.map (fun prop -> (prop.Key, prop.Value))
                    |> readOnlyDict
                else
                    EmptyProps
        }

    let handleCommand xcmd =
        match xcmd with
        | XCommandConnected cmd ->
            Log.Logger.LogInformation("{0} Connected ProtocolVersion: {1} ServerVersion: {2} MaxMessageSize: {3} Brokerless: {4}",
                prefix, cmd.ProtocolVersion, cmd.ServerVersion, cmd.MaxMessageSize, brokerless)
            if cmd.ShouldSerializeMaxMessageSize() && (not brokerless) && maxMessageSize <> cmd.MaxMessageSize then
                initialConnectionTsc.SetException(MaxMessageSizeChanged cmd.MaxMessageSize)
            else
                initialConnectionTsc.SetResult(this)
        | XCommandPartitionedTopicMetadataResponse cmd ->
            if (cmd.ShouldSerializeError()) then
                checkServerError cmd.Error cmd.Message
                handleError %cmd.RequestId cmd.Error cmd.Message
            else
                let result = PartitionedTopicMetadata { Partitions = int cmd.Partitions }
                handleSuccess %cmd.RequestId result
        | XCommandSendReceipt cmd ->
            let producerOperations = producers.[%cmd.ProducerId]
            producerOperations.AckReceived { LedgerId = %(int64 cmd.MessageId.ledgerId); EntryId = %(int64 cmd.MessageId.entryId); SequenceId = %cmd.SequenceId }
        | XCommandSendError cmd ->
            Log.Logger.LogWarning("{0} Received send error from server: {1} : {2}", prefix, cmd.Error, cmd.Message)
            let producerOperations = producers.[%cmd.ProducerId]
            match cmd.Error with
            | ServerError.ChecksumError ->
                producerOperations.RecoverChecksumError %cmd.SequenceId
            | ServerError.TopicTerminatedError ->
                producerOperations.TopicTerminatedError()
            | _ ->
                // By default, for transient error, let the reconnection logic
                // to take place and re-establish the produce again
                this.Close()
        | XCommandPing _ ->
            Commands.newPong() |> SocketMessageWithoutReply |> sendMb.Post
        | XCommandMessage (cmd, metadata, payload, checkSumValid) ->
            let consumerOperations = consumers.[%cmd.ConsumerId]
            let msgReceived = getMessageReceived cmd metadata payload checkSumValid
            consumerOperations.MessageReceived(msgReceived, this)
        | XCommandLookupTopicResponse cmd ->
            if (cmd.ShouldSerializeError()) then
                checkServerError cmd.Error cmd.Message
                handleError %cmd.RequestId cmd.Error cmd.Message
            else
                let result = LookupTopicResult {
                    BrokerServiceUrl = cmd.brokerServiceUrl
                    BrokerServiceUrlTls = cmd.brokerServiceUrlTls
                    Redirect = (cmd.Response = CommandLookupTopicResponse.LookupType.Redirect)
                    Proxy = cmd.ProxyThroughServiceUrl
                    Authoritative = cmd.Authoritative }
                handleSuccess %cmd.RequestId result
        | XCommandProducerSuccess cmd ->            
            let result = ProducerSuccess {
                GeneratedProducerName = cmd.ProducerName
                SchemaVersion = getOptionalSchemaVersion cmd.SchemaVersion
            }
            handleSuccess %cmd.RequestId result
        | XCommandSuccess cmd ->
            handleSuccess %cmd.RequestId Empty
        | XCommandCloseProducer cmd ->
            let producerOperations = producers.[%cmd.ProducerId]
            producerOperations.ConnectionClosed(this)
        | XCommandCloseConsumer cmd ->
            let consumerOperations = consumers.[%cmd.ConsumerId]
            consumerOperations.ConnectionClosed(this)
        | XCommandReachedEndOfTopic cmd ->
            let consumerOperations = consumers.[%cmd.ConsumerId]
            consumerOperations.ReachedEndOfTheTopic()
        | XCommandGetTopicsOfNamespaceResponse cmd ->
            let result = TopicsOfNamespace { Topics = List.ofSeq cmd.Topics }
            handleSuccess %cmd.RequestId result
        | XCommandGetLastMessageIdResponse cmd ->
            let result = LastMessageId {
                LedgerId = %(int64 cmd.LastMessageId.ledgerId)
                EntryId = %(int64 cmd.LastMessageId.entryId)
                Type = Individual
                Partition = cmd.LastMessageId.Partition
                TopicName = %"" }
            handleSuccess %cmd.RequestId result
        | XCommandActiveConsumerChange cmd ->
            let consumerOperations = consumers.[%cmd.ConsumerId]
            consumerOperations.ActiveConsumerChanged(cmd.IsActive)
        | XCommandGetSchemaResponse cmd ->
            if (cmd.ShouldSerializeErrorCode()) then
                if cmd.ErrorCode = ServerError.TopicNotFound then
                    let result = TopicSchema None
                    handleSuccess %cmd.RequestId result
                else
                    checkServerError cmd.ErrorCode cmd.ErrorMessage
                    handleError %cmd.RequestId cmd.ErrorCode cmd.ErrorMessage
            else            
                let result = TopicSchema ({
                    SchemaInfo = getSchemaInfo cmd.Schema
                    SchemaVersion = getOptionalSchemaVersion cmd.SchemaVersion
                } |> Some)
                handleSuccess %cmd.RequestId result
        | XCommandError cmd ->
            Log.Logger.LogError("{0} CommandError Error: {1}. Message: {2}", prefix, cmd.Error, cmd.Message)
            handleError %cmd.RequestId cmd.Error cmd.Message

    let readSocket () =
        task {
            Log.Logger.LogDebug("{0} Started read socket", prefix)
            let mutable continueLooping = true
            let reader = connection.Input

            try
                while continueLooping do
                    let! result = reader.ReadAsync()
                    let buffer = result.Buffer
                    if result.IsCompleted then
                        if initialConnectionTsc.TrySetException(ConnectException("Unable to initiate connection")) then
                            Log.Logger.LogWarning("{0} New connection was aborted", prefix)
                        Log.Logger.LogWarning("{0} Socket was disconnected normally while reading", prefix)
                        operationsMb.Post(ChannelInactive)
                        continueLooping <- false
                    else
                        match tryParse buffer with
                        | Result.Ok xcmd, consumed ->
                            handleCommand xcmd
                            reader.AdvanceTo consumed
                        | Result.Error IncompleteCommand, _ ->
                            Log.Logger.LogDebug("IncompleteCommand received", prefix)
                            reader.AdvanceTo(buffer.Start, buffer.End)
                        | Result.Error (CorruptedCommand ex), consumed ->
                            Log.Logger.LogError(ex, "{0} Ignoring corrupted command.", prefix)
                            reader.AdvanceTo consumed
                        | Result.Error (UnknownCommandType unknownType), consumed ->
                            Log.Logger.LogError("{0} UnknownCommandType {1}, ignoring message", prefix, unknownType)
                            reader.AdvanceTo consumed
            with ex ->
                if initialConnectionTsc.TrySetException(ConnectException("Unable to initiate connection")) then
                    Log.Logger.LogWarning("{0} New connection was aborted", prefix)
                Log.Logger.LogWarning(ex, "{0} Socket was disconnected exceptionally while reading", prefix)
                operationsMb.Post(ChannelInactive)

            Log.Logger.LogDebug("{0} readSocket stopped", prefix)
        } :> Task

    do requestsMb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} requestsMb mailbox failure", prefix))
    do operationsMb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} operationsMb mailbox failure", prefix))
    do sendMb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} sendMb mailbox failure", prefix))
    do Task.Run(fun () -> readSocket()) |> ignore

    member private this.SendMb with get(): MailboxProcessor<SocketMessage> = sendMb

    member private this.OperationsMb with get(): MailboxProcessor<CnxOperation> = operationsMb

    member this.MaxMessageSize with get() = maxMessageSize

    member this.ClientCnxId with get() = clientCnxId

    member this.Send payload =
        sendMb.PostAndAsyncReply(fun replyChannel -> SocketMessageWithReply(payload, replyChannel))

    member this.SendAndWaitForReply reqId payload =
        task {
            let! task = sendMb.PostAndAsyncReply(fun replyChannel -> SocketRequestMessageWithReply(reqId, payload, replyChannel))
            return! task
        }

    member this.RemoveConsumer (consumerId: ConsumerId) =
        operationsMb.Post(RemoveConsumer(consumerId))

    member this.RemoveProducer (consumerId: ProducerId) =
        operationsMb.Post(RemoveProducer(consumerId))

    member this.AddProducer (producerId: ProducerId, producerOperations: ProducerOperations) =
        operationsMb.Post(AddProducer (producerId, producerOperations))

    member this.AddConsumer (consumerId: ConsumerId, consumerOperations: ConsumerOperations) =
        operationsMb.Post(AddConsumer (consumerId, consumerOperations))

    member this.Close() =
        connection.Dispose()

    member this.Dispose() =
        sendMb.Post(SocketMessage.Stop)
        operationsMb.Post(CnxOperation.Stop)
        requestsMb.Post(FailAllRequestsAndStop)
        this.Close()
