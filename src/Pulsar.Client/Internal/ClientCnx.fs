namespace Pulsar.Client.Internal

open System.Reflection
open Pulsar.Client.Common
open System.Collections.Generic

open Microsoft.Extensions.Logging
open System.Threading.Tasks
open Pulsar.Client.Transaction
open pulsar.proto
open System
open FSharp.UMX
open System.Buffers
open System.IO
open ProtoBuf
open System.Threading
open Pulsar.Client.Api
open System.Timers
open System.Threading.Channels

type internal RequestTime =
    {
        RequestStartTime: DateTime
        RequestId: RequestId
        ResponseTcs: TaskCompletionSource<PulsarResponseType>
        CommandType: BaseCommand.Type
    }

type internal ProducerOperations =
    {
        AckReceived: SendReceipt -> unit
        TopicTerminatedError: unit -> unit
        RecoverChecksumError: SequenceId -> unit
        RecoverNotAllowedError: SequenceId -> unit
        ConnectionClosed: ClientCnx -> unit
    }
and internal ConsumerOperations =
    {
        MessageReceived: RawMessage * ClientCnx -> unit
        ReachedEndOfTheTopic: unit -> unit
        ActiveConsumerChanged: bool -> unit
        ConnectionClosed: ClientCnx -> unit
        AckReceipt: RequestId -> unit
        AckError: RequestId * exn -> unit
    }
and internal TransactionMetaStoreOperations =
    {
        NewTxnResponse: RequestId*ResultOrException<TxnId> -> unit
        AddPartitionToTxnResponse: RequestId*ResultOrException<unit> -> unit
        AddSubscriptionToTxnResponse: RequestId*ResultOrException<unit> -> unit
        EndTxnResponse: RequestId*ResultOrException<unit> -> unit
        ConnectionClosed: ClientCnx -> unit
    }

and internal RequestsOperation =
    | AddRequest of RequestId * BaseCommand.Type * TaskCompletionSource<PulsarResponseType>
    | CompleteRequest of RequestId * BaseCommand.Type * PulsarResponseType
    | FailRequest of RequestId * BaseCommand.Type * exn
    | FailAllRequests
    | Stop
    | Tick

and internal CnxOperation =
    | AddProducer of ProducerId * ProducerOperations
    | AddConsumer of ConsumerId * ConsumerOperations
    | AddTransactionMetaStoreHandler of TransactionCoordinatorId * TransactionMetaStoreOperations
    | RemoveConsumer of ConsumerId
    | RemoveProducer of ProducerId
    | RemoveTransactionMetaStoreHandler of TransactionCoordinatorId
    | ChannelInactive
    | Stop
    | Tick

and internal PulsarCommand =
    | XCommandConnected of CommandConnected
    | XCommandPartitionedTopicMetadataResponse of CommandPartitionedTopicMetadataResponse
    | XCommandSendReceipt of CommandSendReceipt
    | XCommandMessage of (CommandMessage * MessageMetadata * byte[] * bool )
    | XCommandPing of CommandPing
    | XCommandPong of CommandPong
    | XCommandLookupResponse of CommandLookupTopicResponse
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
    | XCommandNewTxnResponse of CommandNewTxnResponse
    | XCommandAddPartitionToTxnResponse of CommandAddPartitionToTxnResponse
    | XCommandAddSubscriptionToTxnResponse of CommandAddSubscriptionToTxnResponse
    | XCommandAckResponse of CommandAckResponse
    | XCommandEndTxnResponse of CommandEndTxnResponse
    | XCommandAuthChallenge of CommandAuthChallenge
    | XCommandError of CommandError
    | XCommandTcClientConnectResponse of CommandTcClientConnectResponse

and internal CommandParseError =
    | IncompleteCommand
    | CorruptedCommand of exn
    | UnknownCommandType of BaseCommand.Type

and internal SocketMessage =
    | SocketMessageWithReply of Payload * TaskCompletionSource<bool>
    | SocketMessageWithoutReply of Payload
    | SocketRequestMessageWithReply of RequestId * Payload * TaskCompletionSource<PulsarResponseType>
    | Stop

and internal ClientCnx (config: PulsarClientConfiguration,
                broker: Broker,
                connection: Connection,
                maxMessageSize: int,
                initialConnectionTsc: TaskCompletionSource<ClientCnx>,
                unregisterClientCnx: Broker -> unit) as this =

    let clientVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString()
    let protocolVersion =
        ProtocolVersion.GetValues(typeof<ProtocolVersion>)
        :?> ProtocolVersion[]
        |> Array.last
    let mutable remoteEndpointProtocolVersion = protocolVersion

    let (PhysicalAddress physicalAddress) = broker.PhysicalAddress
    let (LogicalAddress logicalAddress) = broker.LogicalAddress
    let proxyToBroker = if physicalAddress = logicalAddress then None else Some logicalAddress
    let mutable authenticationDataProvider = config.Authentication.GetAuthData(physicalAddress.Host);

    let consumers = Dictionary<ConsumerId, ConsumerOperations>()
    let producers = Dictionary<ProducerId, ProducerOperations>()
    let transactionMetaStores = Dictionary<TransactionCoordinatorId, TransactionMetaStoreOperations>()
    let requests = Dictionary<RequestId, TaskCompletionSource<PulsarResponseType>>()
    let requestTimeoutQueue = Queue<RequestTime>()
    let clientCnxId = Generators.getNextClientCnxId()
    let prefix = $"clientCnx({clientCnxId}, {broker.LogicalAddress})"
    let maxNumberOfRejectedRequestPerConnection = config.MaxNumberOfRejectedRequestPerConnection
    let rejectedRequestResetTimeSec = 60
    let mutable numberOfRejectedRequests = 0
    let mutable isActive = true
    let mutable waitingForPingResponse = false
    let socketReadCancellationTokenSource = new CancellationTokenSource()
    let requestTimeoutTimer = new Timer()
    let keepAliveTimer = new Timer()
    let startRequestTimeoutTimer () =
        requestTimeoutTimer.Interval <- config.OperationTimeout.TotalMilliseconds
        requestTimeoutTimer.AutoReset <- true
        requestTimeoutTimer.Elapsed.Add(fun _ -> post this.RequestsMb RequestsOperation.Tick)
        requestTimeoutTimer.Start()

    let startKeepAliveTimer () =
        keepAliveTimer.Interval <- config.KeepAliveInterval.TotalMilliseconds
        keepAliveTimer.AutoReset <- true
        keepAliveTimer.Elapsed.Add(fun _ -> post this.OperationsMb CnxOperation.Tick)
        keepAliveTimer.Start()

    let failRequest reqId commandType (ex: exn) isTimeout =
        match requests.TryGetValue(reqId) with
        | true, tsc ->
            Log.Logger.LogWarning(ex, "{0} fail request {1} type {2}", prefix, reqId, commandType)
            tsc.SetException ex
            requests.Remove reqId |> ignore
        | _ ->
            if not isTimeout then
                Log.Logger.LogWarning(ex, "{0} fail non-existent request {1} type {2}, ignoring", prefix, reqId, commandType)

    let rec handleTimeoutedMessages() =
        if requestTimeoutQueue.Count > 0 then
            let request = requestTimeoutQueue.Peek()
            let currentDiff = DateTime.Now - request.RequestStartTime
            if currentDiff >= config.OperationTimeout then
                let request = requestTimeoutQueue.Dequeue()
                let ex = TimeoutException <| String.Format("{0} request {1} type {2} timed out after {3}ms",
                                                prefix, request.RequestId, request.CommandType, currentDiff.TotalMilliseconds)
                failRequest request.RequestId request.CommandType ex true
                handleTimeoutedMessages()
            else
                // if there is no request that is timed out then exit the loop
                ()

    let handleKeepAliveTimeout() =
        if this.WaitingForPingResponse then
            connection.Dispose()
        else
            this.WaitingForPingResponse <- true
            post this.SendMb (SocketMessageWithoutReply(Commands.newPing()))

    let getTransactionExceptionByServerError error msg =
        match error with
        | ServerError.TransactionCoordinatorNotFound -> CoordinatorNotFoundException msg :> exn
        | ServerError.InvalidTxnStatus -> InvalidTxnStatusException msg :> exn
        | ServerError.TransactionNotFound -> TransactionNotFoundException msg :> exn
        | _ -> Exception(msg)

    let failAllRequests() =
        for KeyValue(_, rspTask) in requests do
            rspTask.SetException(ConnectException "Disconnected.")
        requests.Clear()

    let requestsMb = Channel.CreateUnbounded<RequestsOperation>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! requestsMb.Reader.ReadAsync() with
            | AddRequest (reqId, commandType, tsc) ->
                Log.Logger.LogDebug("{0} add request {1} type {2}", prefix, reqId, commandType)
                requests.Add(reqId, tsc)
                requestTimeoutQueue.Enqueue({ RequestStartTime = DateTime.Now; RequestId = reqId; ResponseTcs = tsc; CommandType = commandType })
            | CompleteRequest (reqId, commandType, result) ->
                match requests.TryGetValue(reqId) with
                | true, tsc ->
                    Log.Logger.LogDebug("{0} complete request {1} type {2}", prefix, reqId, commandType)
                    tsc.SetResult result
                    requests.Remove reqId |> ignore
                | _ ->
                    Log.Logger.LogWarning("{0} complete non-existent request {1} type {2}, ignoring", prefix, reqId, commandType)
            | FailRequest (reqId, commandType, ex) ->
                failRequest reqId commandType ex false
            | FailAllRequests ->
                Log.Logger.LogDebug("{0} fail requests", prefix)
                failAllRequests()
            | RequestsOperation.Stop ->
                Log.Logger.LogDebug("{0} has stopped", prefix)
                failAllRequests()
                requestTimeoutTimer.Stop()
                continueLoop <- false
            | RequestsOperation.Tick ->
                Log.Logger.LogTrace("{0} timeout tick", prefix)
                handleTimeoutedMessages()
        } :> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} requestsMb mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} requestsMb mailbox has stopped normally", prefix))
        |> ignore

    let tryStopMailboxes() =
        if consumers.Count = 0 && producers.Count = 0 && transactionMetaStores.Count = 0 then
            post this.SendMb SocketMessage.Stop
            post this.OperationsMb CnxOperation.Stop
            post this.RequestsMb RequestsOperation.Stop

    let operationsMb = Channel.CreateUnbounded<CnxOperation>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! operationsMb.Reader.ReadAsync() with
            | AddProducer (producerId, producerOperation) ->
                Log.Logger.LogDebug("{0} adding producer {1}", prefix, producerId)
                producers.Add(producerId, producerOperation)
            | AddConsumer (consumerId, consumerOperation) ->
                Log.Logger.LogDebug("{0} adding consumer {1}", prefix, consumerId)
                consumers.Add(consumerId, consumerOperation)
            | AddTransactionMetaStoreHandler (transactionMetaStoreId, transactionMetaStoreOperations) ->
                Log.Logger.LogDebug("{0} adding transaction metastore {1}", prefix, transactionMetaStoreId)
                transactionMetaStores[transactionMetaStoreId] <- transactionMetaStoreOperations
            | RemoveConsumer consumerId ->
                Log.Logger.LogDebug("{0} removing consumer {1}", prefix, consumerId)
                consumers.Remove(consumerId) |> ignore
                if this.IsActive |> not then
                    tryStopMailboxes()
            | RemoveProducer producerId ->
                Log.Logger.LogDebug("{0} removing producer {1}", prefix, producerId)
                producers.Remove(producerId) |> ignore
                if this.IsActive |> not then
                    tryStopMailboxes()
            | RemoveTransactionMetaStoreHandler transactionMetaStoreId ->
                Log.Logger.LogDebug("{0} removing transactionMetaStore {1}", prefix, transactionMetaStoreId)
                transactionMetaStores.Remove(transactionMetaStoreId) |> ignore
                if this.IsActive |> not then
                    tryStopMailboxes()
            | ChannelInactive ->
                if this.IsActive then
                    this.IsActive <- false
                    Log.Logger.LogDebug("{0} ChannelInactive", prefix)
                    unregisterClientCnx(broker)
                    this.Dispose()
                    consumers |> Seq.iter(fun (KeyValue(_,consumerOperation)) ->
                        consumerOperation.ConnectionClosed(this))
                    producers |> Seq.iter(fun (KeyValue(_,producerOperation)) ->
                        producerOperation.ConnectionClosed(this))
                    transactionMetaStores |> Seq.iter(fun (KeyValue(_,transactionMetaStoreOperation)) ->
                        transactionMetaStoreOperation.ConnectionClosed(this))
                    post requestsMb FailAllRequests
                tryStopMailboxes()

            | CnxOperation.Stop ->
                Log.Logger.LogDebug("{0} operationsMb stopped", prefix)
                keepAliveTimer.Stop()
                continueLoop <- false
            | CnxOperation.Tick ->
                Log.Logger.LogDebug("{0} keepalive tick", prefix)
                handleKeepAliveTimeout()
        } :> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} operationsMb mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} operationsMb mailbox has stopped normally", prefix))
    |> ignore

    let sendSerializedPayload (writePayload, commandType: BaseCommand.Type) =
        Log.Logger.LogDebug("{0} Sending message of type {1}", prefix, commandType)
        backgroundTask {
            try
                do! connection.Output |> writePayload
                return true
            with ex ->
                Log.Logger.LogWarning(ex, "{0} Socket was disconnected exceptionally on writing {1}", prefix, commandType)
                post operationsMb ChannelInactive
                return false
        }

    let sendMb = Channel.CreateUnbounded<SocketMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! sendMb.Reader.ReadAsync() with
            | SocketMessageWithReply (payload, channel) ->
                let! connected = sendSerializedPayload payload
                channel.SetResult(connected)
            | SocketMessageWithoutReply payload ->
                let! _ = sendSerializedPayload payload
                ()
            | SocketRequestMessageWithReply (reqId, payload, channel) ->
                post requestsMb (AddRequest(reqId, snd payload, channel))
                let! _ = sendSerializedPayload payload
                ()
            | SocketMessage.Stop ->
                Log.Logger.LogDebug("{0} sendMb stopped", prefix)
                continueLoop <- false
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} sendMb mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} sendMb mailbox has stopped normally", prefix))
    |> ignore

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
            let metadata,payload,checksumValid = readMessage reader stream frameLength
            Ok (XCommandMessage (command.Message, metadata, payload, checksumValid))
        | BaseCommand.Type.LookupResponse ->
            Ok (XCommandLookupResponse command.lookupTopicResponse)
        | BaseCommand.Type.Ping ->
            Ok (XCommandPing command.Ping)
        | BaseCommand.Type.Pong ->
            Ok (XCommandPong command.Pong)
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
        | BaseCommand.Type.AckResponse ->
            Ok (XCommandAckResponse command.ackResponse)
        | BaseCommand.Type.NewTxnResponse ->
            Ok (XCommandNewTxnResponse command.newTxnResponse)
        | BaseCommand.Type.AddPartitionToTxnResponse ->
            Ok (XCommandAddPartitionToTxnResponse command.addPartitionToTxnResponse)
        | BaseCommand.Type.AddSubscriptionToTxnResponse ->
            Ok (XCommandAddSubscriptionToTxnResponse command.addSubscriptionToTxnResponse)
        | BaseCommand.Type.EndTxnResponse ->
            Ok (XCommandEndTxnResponse command.endTxnResponse)
        | BaseCommand.Type.AuthChallenge ->
            Ok (XCommandAuthChallenge command.authChallenge)
        | BaseCommand.Type.Error ->
            Ok (XCommandError command.Error)
        | BaseCommand.Type.TcClientConnectResponse ->
            Ok (XCommandTcClientConnectResponse command.tcClientConnectResponse)
        | unknownType ->
            Result.Error (UnknownCommandType unknownType)

    let tryParse (buffer: ReadOnlySequence<byte>) =
        let length = int buffer.Length
        if (length >= 8) then
            let array = ArrayPool.Shared.Rent length
            try
                buffer.CopyTo(Span(array))
                use stream =  new MemoryStream(array)
                use reader = new BinaryReader(stream)
                let totalength = reader.ReadInt32() |> int32FromBigEndian
                let frameLength = totalength + 4
                if (length >= frameLength) then
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
            finally
                ArrayPool.Shared.Return array
        else
            Result.Error IncompleteCommand, SequencePosition()


    let handleSuccess requestId result responseType =
        post requestsMb (CompleteRequest(requestId, responseType, result))

    let getPulsarClientException error errorMsg =
        match error with
        | ServerError.MetadataError -> BrokerMetadataException errorMsg :> exn
        | ServerError.PersistenceError -> BrokerPersistenceException errorMsg :> exn
        | ServerError.AuthenticationError -> AuthenticationException errorMsg :> exn
        | ServerError.AuthorizationError -> AuthorizationException errorMsg :> exn
        | ServerError.ConsumerBusy -> ConsumerBusyException errorMsg :> exn
        | ServerError.ServiceNotReady -> LookupException errorMsg :> exn
        | ServerError.ProducerBlockedQuotaExceededError -> ProducerBlockedQuotaExceededError errorMsg :> exn
        | ServerError.ProducerBlockedQuotaExceededException -> ProducerBlockedQuotaExceededException errorMsg :> exn
        | ServerError.ChecksumError -> ChecksumException errorMsg :> exn
        | ServerError.UnsupportedVersionError -> UnsupportedVersionException errorMsg :> exn
        | ServerError.TopicNotFound -> TopicDoesNotExistException errorMsg :> exn
        | ServerError.SubscriptionNotFound -> SubscriptionNotFoundException errorMsg :> exn
        | ServerError.ConsumerNotFound -> ConsumerNotFoundException errorMsg :> exn
        | ServerError.TooManyRequests -> TooManyRequestsException errorMsg :> exn
        | ServerError.TopicTerminatedError -> TopicTerminatedException errorMsg :> exn
        | ServerError.ProducerBusy -> ProducerBusyException errorMsg :> exn
        | ServerError.InvalidTopicName -> InvalidTopicNameException errorMsg :> exn
        | ServerError.IncompatibleSchema -> IncompatibleSchemaException errorMsg :> exn
        | ServerError.ConsumerAssignError -> ConsumerAssignException errorMsg :> exn
        | ServerError.TransactionCoordinatorNotFound -> CoordinatorNotFoundException errorMsg :> exn
        | ServerError.InvalidTxnStatus -> InvalidTxnStatusException errorMsg :> exn
        | ServerError.NotAllowedError -> NotAllowedException errorMsg :> exn
        | ServerError.TransactionConflict -> TransactionConflictException errorMsg :> exn
        | _ -> exn errorMsg

    let handleError requestId error msg commandType =
        let ex = getPulsarClientException error msg
        post requestsMb (FailRequest(requestId, commandType, ex))

    let handleTransactionError requestId error msg commandType =
        let ex = getTransactionExceptionByServerError error msg
        post requestsMb (FailRequest(requestId, commandType, ex))

    let checkServerError serverError errMsg =
        if (serverError = ServerError.ServiceNotReady) then
            Log.Logger.LogError("{0} Close connection because received internal-server error {1}", prefix, errMsg);
            this.Dispose()
        elif (serverError = ServerError.TooManyRequests) then
            let rejectedRequests = Interlocked.Increment(&numberOfRejectedRequests)
            if (rejectedRequests = 1) then
                // schedule timer
                asyncDelayMs (rejectedRequestResetTimeSec*1000) (fun() -> Interlocked.Exchange(&numberOfRejectedRequests, 0) |> ignore)
            elif (rejectedRequests >= maxNumberOfRejectedRequestPerConnection) then
                Log.Logger.LogError("{0} Close connection because received {1} rejected request in {2} seconds ", prefix,
                        rejectedRequests, rejectedRequestResetTimeSec);
                this.Dispose()

    let getOptionalSchemaVersion =
        Option.ofObj >> Option.map (fun bytes -> { SchemaVersion.Bytes = bytes })

    let getMessageReceived (cmd: CommandMessage) (messageMetadata: MessageMetadata) payload checkSumValid =
        let mapCompressionType = function
            | CompressionType.None -> Pulsar.Client.Common.CompressionType.None
            | CompressionType.Lz4 -> Pulsar.Client.Common.CompressionType.LZ4
            | CompressionType.Zlib -> Pulsar.Client.Common.CompressionType.ZLib
            | CompressionType.Zstd -> Pulsar.Client.Common.CompressionType.ZStd
            | CompressionType.Snappy -> Pulsar.Client.Common.CompressionType.Snappy
            | _ -> Pulsar.Client.Common.CompressionType.None
        let metadata = {
            NumMessages = messageMetadata.NumMessagesInBatch
            NumChunks = messageMetadata.NumChunksFromMsg
            TotalChunkMsgSize = messageMetadata.TotalChunkMsgSize
            HasNumMessagesInBatch = messageMetadata.ShouldSerializeNumMessagesInBatch()
            CompressionType = messageMetadata.Compression |> mapCompressionType
            UncompressedMessageSize = messageMetadata.UncompressedSize |> int32
            SchemaVersion = getOptionalSchemaVersion messageMetadata.SchemaVersion
            SequenceId = %(int64 messageMetadata.SequenceId)
            ChunkId = %(int messageMetadata.ChunkId)
            PublishTime = %(int64 messageMetadata.PublishTime)
            Uuid = %messageMetadata.Uuid
            EventTime =
                if messageMetadata.ShouldSerializeEventTime() then
                    %(int64 messageMetadata.EventTime) |> Nullable
                else
                    Nullable()
            EncryptionKeys =
                if messageMetadata.EncryptionKeys.Count > 0 then
                    messageMetadata.EncryptionKeys |> Seq.map EncryptionKey.FromProto |> Seq.toArray
                else
                    [||]
            EncryptionParam = messageMetadata.EncryptionParam
            EncryptionAlgo = messageMetadata.EncryptionAlgo
            OrderingKey = messageMetadata.OrderingKey
            ReplicatedFrom = messageMetadata.ReplicatedFrom
        }

        {
            MessageId =
                {
                    LedgerId = %(int64 cmd.MessageId.ledgerId)
                    EntryId = %(int64 cmd.MessageId.entryId)
                    Type = MessageIdType.Single
                    Partition = -1
                    TopicName = %""
                    ChunkMessageIds = None
                }
            RedeliveryCount = int cmd.RedeliveryCount
            Metadata = metadata
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
            AckSet =
                if isNull cmd.AckSets then
                    EmptyAckSet
                else
                    fromLongArray cmd.AckSets metadata.NumMessages
        }

    let handleCommand xcmd =
        this.WaitingForPingResponse <- false
        match xcmd with
        | XCommandConnected cmd ->
            Log.Logger.LogInformation("{0} Connected ProtocolVersion: {1} ServerVersion: {2} MaxMessageSize: {3}",
                prefix, cmd.ProtocolVersion, cmd.ServerVersion, cmd.MaxMessageSize)
            if cmd.ShouldSerializeMaxMessageSize() && maxMessageSize <> cmd.MaxMessageSize then
                initialConnectionTsc.SetException(MaxMessageSizeChanged cmd.MaxMessageSize)
            else
                remoteEndpointProtocolVersion <- enum cmd.ProtocolVersion
                initialConnectionTsc.SetResult(this)
        | XCommandPartitionedTopicMetadataResponse cmd ->
            if cmd.ShouldSerializeError() then
                checkServerError cmd.Error cmd.Message
                handleError %cmd.RequestId cmd.Error cmd.Message BaseCommand.Type.PartitionedMetadataResponse
            else
                let result = PartitionedTopicMetadata { Partitions = int cmd.Partitions }
                handleSuccess %cmd.RequestId result BaseCommand.Type.PartitionedMetadataResponse
        | XCommandSendReceipt cmd ->
            match producers.TryGetValue %cmd.ProducerId with
            | true, producerOperations ->
                producerOperations.AckReceived {
                    LedgerId = %(int64 cmd.MessageId.ledgerId)
                    EntryId = %(int64 cmd.MessageId.entryId)
                    SequenceId = %(int64 cmd.SequenceId)
                    HighestSequenceId = %(int64 cmd.HighestSequenceId)
                }
            | _ ->
                Log.Logger.LogWarning("{0} producer {1} wasn't found on CommandSendReceipt", prefix, %cmd.ProducerId)
        | XCommandAckResponse cmd ->
            match consumers.TryGetValue %cmd.ConsumerId with
            | true, consumerOperations ->
                if cmd.ShouldSerializeError() then
                    let ex = getPulsarClientException cmd.Error cmd.Message
                    consumerOperations.AckError(%cmd.RequestId, ex)
                else
                    consumerOperations.AckReceipt(%cmd.RequestId)
            | _ ->
                Log.Logger.LogWarning("{0} consumer {1} wasn't found on CommandAckResponse", prefix, %cmd.ConsumerId)
        | XCommandSendError cmd ->
            Log.Logger.LogWarning("{0} Received send error from server: {1} : {2}", prefix, cmd.Error, cmd.Message)
            match producers.TryGetValue %cmd.ProducerId with
            | true, producerOperations ->
                match cmd.Error with
                | ServerError.ChecksumError ->
                    producerOperations.RecoverChecksumError %(int64 cmd.SequenceId)
                | ServerError.TopicTerminatedError ->
                    producerOperations.TopicTerminatedError()
                | ServerError.NotAllowedError ->
                    producerOperations.RecoverNotAllowedError %(int64 cmd.SequenceId)
                | _ ->
                    // By default, for transient error, let the reconnection logic
                    // to take place and re-establish the produce again
                    this.Dispose()
            | _ ->
                Log.Logger.LogWarning("{0} producer {1} wasn't found on CommandSendError", prefix, %cmd.ProducerId)
        | XCommandPing _ ->
            post sendMb (SocketMessageWithoutReply(Commands.newPong()))
        | XCommandPong _ ->
            ()
        | XCommandMessage (cmd, metadata, payload, checkSumValid) ->
            match consumers.TryGetValue %cmd.ConsumerId with
            | true, consumerOperations ->
                let msgReceived = getMessageReceived cmd metadata payload checkSumValid
                consumerOperations.MessageReceived(msgReceived, this)
            | _ ->
                Log.Logger.LogWarning("{0} consumer {1} wasn't found on CommandMessage", prefix, %cmd.ConsumerId)
        | XCommandLookupResponse cmd ->
            if cmd.ShouldSerializeError() then
                checkServerError cmd.Error cmd.Message
                handleError %cmd.RequestId cmd.Error cmd.Message BaseCommand.Type.LookupResponse
            else
                let result = LookupTopicResult {
                    BrokerServiceUrl = cmd.brokerServiceUrl
                    BrokerServiceUrlTls = cmd.brokerServiceUrlTls
                    Redirect = (cmd.Response = CommandLookupTopicResponse.LookupType.Redirect)
                    Proxy = cmd.ProxyThroughServiceUrl
                    Authoritative = cmd.Authoritative }
                handleSuccess %cmd.RequestId result BaseCommand.Type.LookupResponse
        | XCommandProducerSuccess cmd ->
            let result = ProducerSuccess {
                GeneratedProducerName = cmd.ProducerName
                SchemaVersion = getOptionalSchemaVersion cmd.SchemaVersion
                LastSequenceId = %cmd.LastSequenceId
            }
            handleSuccess %cmd.RequestId result BaseCommand.Type.ProducerSuccess
        | XCommandSuccess cmd ->
            handleSuccess %cmd.RequestId Empty BaseCommand.Type.Success
        | XCommandCloseProducer cmd ->
            match producers.TryGetValue %cmd.ProducerId with
            | true, producerOperations ->
                producerOperations.ConnectionClosed(this)
            | _ ->
                Log.Logger.LogWarning("{0} producer {1} wasn't found on CommandCloseProducer", prefix, %cmd.ProducerId)
        | XCommandCloseConsumer cmd ->
            match consumers.TryGetValue %cmd.ConsumerId with
            | true, consumerOperations ->
                consumerOperations.ConnectionClosed(this)
            | _ ->
                Log.Logger.LogWarning("{0} consumer {1} wasn't found on CommandCloseConsumer", prefix, %cmd.ConsumerId)
        | XCommandReachedEndOfTopic cmd ->
            match consumers.TryGetValue %cmd.ConsumerId with
            | true, consumerOperations ->
                consumerOperations.ReachedEndOfTheTopic()
            | _ ->
                Log.Logger.LogWarning("{0} consumer {1} wasn't found on CommandReachedEndOfTopic", prefix, %cmd.ConsumerId)
        | XCommandGetTopicsOfNamespaceResponse cmd ->
            let result = TopicsOfNamespace cmd.Topics
            handleSuccess %cmd.RequestId result BaseCommand.Type.GetTopicsOfNamespaceResponse
        | XCommandGetLastMessageIdResponse cmd ->
            let lastMessageId = {
                LedgerId = %(int64 cmd.LastMessageId.ledgerId)
                EntryId = %(int64 cmd.LastMessageId.entryId)
                Type =
                    match cmd.LastMessageId.BatchIndex with
                    | index when index >= 0  -> Batch(%index, BatchMessageAcker.NullAcker)
                    | _ -> MessageIdType.Single
                Partition = cmd.LastMessageId.Partition
                TopicName = %""
                ChunkMessageIds = None
            }
            let markDeletePosition =
                cmd.ConsumerMarkDeletePosition
                |> Option.ofObj
                |> Option.map (fun msgIdData ->
                    {
                        LedgerId = %(int64 msgIdData.ledgerId)
                        EntryId = %(int64 msgIdData.entryId)
                        Type = MessageIdType.Single
                        Partition = -1
                        TopicName = %""
                        ChunkMessageIds = None
                    })
            let result = LastMessageId {
                LastMessageId = lastMessageId
                MarkDeletePosition = markDeletePosition
            }
            handleSuccess %cmd.RequestId result BaseCommand.Type.GetLastMessageIdResponse
        | XCommandActiveConsumerChange cmd ->
            match consumers.TryGetValue %cmd.ConsumerId with
            | true, consumerOperations ->
                consumerOperations.ActiveConsumerChanged(cmd.IsActive)
            | _ ->
                Log.Logger.LogWarning("{0} consumer {1} wasn't found on CommandActiveConsumerChange", prefix, %cmd.ConsumerId)
        | XCommandGetSchemaResponse cmd ->
            if cmd.ShouldSerializeErrorCode() then
                if cmd.ErrorCode = ServerError.TopicNotFound then
                    let result = TopicSchema None
                    handleSuccess %cmd.RequestId result BaseCommand.Type.GetSchemaResponse
                else
                    checkServerError cmd.ErrorCode cmd.ErrorMessage
                    handleError %cmd.RequestId cmd.ErrorCode cmd.ErrorMessage BaseCommand.Type.GetSchemaResponse
            else
                let result = TopicSchema ({
                    SchemaInfo = getSchemaInfo cmd.Schema
                    SchemaVersion = getOptionalSchemaVersion cmd.SchemaVersion
                } |> Some)
                handleSuccess %cmd.RequestId result BaseCommand.Type.GetSchemaResponse
        | XCommandNewTxnResponse cmd ->
            let tcId = %cmd.TxnidMostBits
            match transactionMetaStores.TryGetValue tcId with
            | true, tmsOperations ->
                let result =
                    if cmd.ShouldSerializeError() then
                        getTransactionExceptionByServerError cmd.Error cmd.Message
                        |> Error
                    else
                         {
                            LeastSigBits = cmd.TxnidLeastBits
                            MostSigBits = cmd.TxnidMostBits
                         } |> Ok
                tmsOperations.NewTxnResponse (%cmd.RequestId, result)
            | _ ->
                Log.Logger.LogWarning("{0} tms {1} wasn't found on CommandNewTxnResponse", prefix, tcId)
        | XCommandAddPartitionToTxnResponse cmd ->
            let tcId = %cmd.TxnidMostBits
            match transactionMetaStores.TryGetValue tcId with
            | true, tmsOperations ->
                let result =
                    if cmd.ShouldSerializeError() then
                        getTransactionExceptionByServerError cmd.Error cmd.Message
                        |> Error
                    else
                         Ok ()
                tmsOperations.AddPartitionToTxnResponse (%cmd.RequestId, result)
            | _ ->
                Log.Logger.LogWarning("{0} tms {1} wasn't found on CommandAddPartitionToTxnResponse", prefix, tcId)
        | XCommandAddSubscriptionToTxnResponse cmd ->
            let tcId = %cmd.TxnidMostBits
            match transactionMetaStores.TryGetValue tcId with
            | true, tmsOperations ->
                let result =
                    if cmd.ShouldSerializeError() then
                        getTransactionExceptionByServerError cmd.Error cmd.Message
                        |> Error
                    else
                         Ok ()
                tmsOperations.AddSubscriptionToTxnResponse (%cmd.RequestId, result)
            | _ ->
                Log.Logger.LogWarning("{0} tms {1} wasn't found on XCommandAddSubscriptionToTxnResponse", prefix, tcId)
        | XCommandEndTxnResponse cmd ->
            let tcId = %cmd.TxnidMostBits
            match transactionMetaStores.TryGetValue tcId with
            | true, tmsOperations ->
                let result =
                    if cmd.ShouldSerializeError() then
                        getTransactionExceptionByServerError cmd.Error cmd.Message
                        |> Error
                    else
                         Ok ()
                tmsOperations.EndTxnResponse (%cmd.RequestId, result)
            | _ ->
                Log.Logger.LogWarning("{0} tms {1} wasn't found on XCommandAddSubscriptionToTxnResponse", prefix, tcId)
        | XCommandAuthChallenge cmd ->
            if cmd.Challenge |> isNull |> not then
                if (cmd.Challenge.auth_data |> Array.compareWith Operators.compare AuthData.REFRESH_AUTH_DATA_BYTES) = 0 then
                    Log.Logger.LogDebug("{0} Refreshed authentication provider", prefix)
                    authenticationDataProvider <- config.Authentication.GetAuthData(physicalAddress.Host)
                let methodName = config.Authentication.GetAuthMethodName()
                let authData = authenticationDataProvider.Authenticate({ Bytes = cmd.Challenge.auth_data })
                let request = Commands.newAuthResponse methodName authData (int protocolVersion) clientVersion
                Log.Logger.LogInformation("{0} Mutual auth {1}, requested {2}", prefix, methodName, cmd.Challenge.AuthMethodName)
                backgroundTask {
                    let! result = this.Send(request)
                    if not result then
                        Log.Logger.LogWarning("{0} Failed to send request for mutual auth to broker", prefix)
                        NotConnectedException "Failed to send request for mutual auth to broker"
                        |> initialConnectionTsc.TrySetException
                        |> ignore
                } |> ignore
            else
                Log.Logger.LogWarning("{0} CommandAuthChallenge with empty challenge", prefix)
        | XCommandError cmd ->
            Log.Logger.LogError("{0} CommandError Error: {1}. Message: {2}", prefix, cmd.Error, cmd.Message)
            handleError %cmd.RequestId cmd.Error cmd.Message BaseCommand.Type.Error
        | XCommandTcClientConnectResponse cmd ->
            Log.Logger.LogDebug("{0} Received tc client connect response", prefix)
            if cmd.ShouldSerializeError() then
                handleTransactionError %cmd.RequestId cmd.Error cmd.Message BaseCommand.Type.TcClientConnectResponse
            else
                handleSuccess %cmd.RequestId Empty BaseCommand.Type.TcClientConnectResponse



    let readSocket () =
        backgroundTask {
            Log.Logger.LogDebug("{0} Started read socket", prefix)
            let mutable continueLooping = true
            let reader = connection.Input

            try
                while continueLooping do
                    let! result = reader.ReadAsync(socketReadCancellationTokenSource.Token)
                    let buffer = result.Buffer
                    if result.IsCompleted then
                        if initialConnectionTsc.TrySetException(ConnectException("Unable to initiate connection")) then
                            Log.Logger.LogWarning("{0} New connection was aborted", prefix)
                        Log.Logger.LogWarning("{0} Socket was disconnected normally while reading", prefix)
                        post operationsMb ChannelInactive
                        continueLooping <- false
                    else
                        match tryParse buffer with
                        | Result.Ok xcmd, consumed ->
                            handleCommand xcmd
                            reader.AdvanceTo consumed
                        | Result.Error IncompleteCommand, _ ->
                            Log.Logger.LogDebug("{0} IncompleteCommand received", prefix)
                            reader.AdvanceTo(buffer.Start, buffer.End)
                        | Result.Error (CorruptedCommand ex), consumed ->
                            Log.Logger.LogError(ex, "{0} Ignoring corrupted command.", prefix)
                            reader.AdvanceTo consumed
                        | Result.Error (UnknownCommandType unknownType), consumed ->
                            Log.Logger.LogError("{0} UnknownCommandType {1}, ignoring message", prefix, unknownType)
                            reader.AdvanceTo consumed
            with Flatten ex ->
                match ex with
                | :? OperationCanceledException ->
                    Log.Logger.LogInformation("{0} Socket read was cancelled", prefix)
                | _ ->
                    if initialConnectionTsc.TrySetException(ConnectException("Unable to initiate connection")) then
                        Log.Logger.LogWarning("{0} New connection was aborted", prefix)
                    Log.Logger.LogWarning(ex, "{0} Socket was disconnected exceptionally while reading", prefix)
                post operationsMb ChannelInactive
            Log.Logger.LogDebug("{0} readSocket stopped", prefix)
        } :> Task

    do startRequestTimeoutTimer()
    do startKeepAliveTimer()
    do readSocket() |> ignore

    member private this.SendMb = sendMb

    member private this.RequestsMb = requestsMb

    member private this.OperationsMb = operationsMb

    member this.MaxMessageSize = maxMessageSize

    member this.ClientCnxId = clientCnxId

    member this.RemoteEndpointProtocolVersion = remoteEndpointProtocolVersion

    member this.IsActive
        with get() = Volatile.Read(&isActive)
        and private set value = Volatile.Write(&isActive, value)

    member this.WaitingForPingResponse
        with get() = Volatile.Read(&waitingForPingResponse)
        and private set value = Volatile.Write(&waitingForPingResponse, value)


    member internal this.NewConnectCommand() =
        let authData = authenticationDataProvider.Authenticate(AuthData.INIT_AUTH_DATA)
        let authMethodName = config.Authentication.GetAuthMethodName()
        Commands.newConnect authMethodName authData clientVersion protocolVersion proxyToBroker

    member this.Send payload =
        if this.IsActive then
            postAndAsyncReply sendMb (fun replyChannel -> SocketMessageWithReply(payload, replyChannel))
        else
            falseTask

    member this.SendAndForget (payload: Payload) =
        if this.IsActive then
            post sendMb (SocketMessageWithoutReply payload)

    member this.SendAndWaitForReply reqId payload =
        if this.IsActive then
            postAndAsyncReply sendMb (fun replyChannel -> SocketRequestMessageWithReply(reqId, payload, replyChannel))
        else
            Task.FromException<PulsarResponseType> <| ConnectException "Disconnected."

    member this.RemoveConsumer (consumerId: ConsumerId) =
        if this.IsActive then
            post operationsMb (RemoveConsumer(consumerId))

    member this.RemoveProducer (consumerId: ProducerId) =
        if this.IsActive then
            post operationsMb (RemoveProducer(consumerId))

    member this.RemoveTransactionMetaStoreHandler (transactionMetaStoreId: TransactionCoordinatorId) =
        if this.IsActive then
            post operationsMb (RemoveTransactionMetaStoreHandler(transactionMetaStoreId))

    member this.AddProducer (producerId: ProducerId, producerOperations: ProducerOperations) =
        if this.IsActive then
            post operationsMb (AddProducer (producerId, producerOperations))

    member this.AddConsumer (consumerId: ConsumerId, consumerOperations: ConsumerOperations) =
        if this.IsActive then
            post operationsMb (AddConsumer (consumerId, consumerOperations))

    member this.AddTransactionMetaStoreHandler(transactionMetaStoreId: TransactionCoordinatorId,
                                               transactionMetaStoreOperations: TransactionMetaStoreOperations) =
        if this.IsActive then
            post operationsMb (AddTransactionMetaStoreHandler (transactionMetaStoreId, transactionMetaStoreOperations))

    member this.Dispose() =
        socketReadCancellationTokenSource.Cancel()
        connection.Dispose()

    override this.ToString() =
        prefix
