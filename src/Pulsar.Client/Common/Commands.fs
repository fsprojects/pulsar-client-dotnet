module internal Pulsar.Client.Common.Commands

open System.Collections
open System.Collections.Generic
open Pulsar.Client.Transaction
open pulsar.proto
open FSharp.UMX
open System
open ProtoBuf
open System.IO
open System.Net
open Pulsar.Client.Internal
open Pulsar.Client.Api
open Pulsar.Client.Common


type CommandType = BaseCommand.Type

[<Literal>]
let DEFAULT_MAX_MESSAGE_SIZE = 5_242_880 //5 * 1024 * 1024


let private processSimpleCommand (command : BaseCommand) (stream: Stream) (binaryWriter: BinaryWriter) (output: Stream) =
    // write fake totalLength
    for i in 1..4 do
        stream.WriteByte(0uy)

    // write commandPayload
    Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)
    let frameSize = int stream.Length

    let totalSize = frameSize - 4

    //write total size and command size
    stream.Seek(0L,SeekOrigin.Begin) |> ignore
    binaryWriter.Write(int32ToBigEndian totalSize)
    stream.Seek(0L, SeekOrigin.Begin) |> ignore

    stream.CopyToAsync(output)

let private processComplexCommand (command : BaseCommand) (metadata: MessageMetadata) (payload: byte[])
                                    (stream: Stream) (binaryWriter: BinaryWriter) (output: Stream) =
    // write fake totalLength
    for i in 1..4 do
        stream.WriteByte(0uy)

    // write commandPayload
    Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)

    let stream1Size = int stream.Length

    // write magic number 0x0e01
    stream.WriteByte(14uy)
    stream.WriteByte(1uy)

    // write fake CRC sum
    for i in 1..4 do
        stream.WriteByte(0uy)

    // write metadata
    Serializer.SerializeWithLengthPrefix(stream, metadata, PrefixStyle.Fixed32BigEndian)
    let stream2Size = int stream.Length
    let totalMetadataSize = stream2Size - stream1Size - 6

    // write payload
    stream.Write(payload, 0, payload.Length)

    let frameSize = int stream.Length
    let totalSize = frameSize - 4
    let payloadSize = frameSize - stream2Size

    let crcStart = stream1Size + 2
    let crcPayloadStart = crcStart + 4

    //write CRC
    stream.Seek(int64 crcPayloadStart, SeekOrigin.Begin) |> ignore
    let crc = int32 <| CRC32C.Get(0u, stream, totalMetadataSize + payloadSize)
    stream.Seek(int64 crcStart, SeekOrigin.Begin) |> ignore
    binaryWriter.Write(int32ToBigEndian crc)

    //write total size and command size
    stream.Seek(0L, SeekOrigin.Begin) |> ignore
    binaryWriter.Write(int32ToBigEndian totalSize)

    stream.Seek(0L, SeekOrigin.Begin) |> ignore
    stream.CopyToAsync(output)

let serializeSimpleCommand(command : BaseCommand) =
    let f =
        fun (output: Stream) ->
            backgroundTask {
                use stream = MemoryStreamManager.GetStream()
                use binaryWriter = new BinaryWriter(stream)
                return! processSimpleCommand command stream binaryWriter output
            }
    (f, command.``type``)


let serializePayloadCommand (command : BaseCommand) (metadata: MessageMetadata) (payload: byte[]) =
    let f =
        fun (output: Stream) ->
            backgroundTask {
                use stream = MemoryStreamManager.GetStream()
                use binaryWriter = new BinaryWriter(stream)
                return! processComplexCommand command metadata payload stream binaryWriter output
            }
    (f, command.``type``)

let newPartitionMetadataRequest(topicName : CompleteTopicName) (requestId : RequestId) : Payload =
    let request = CommandPartitionedTopicMetadata(Topic = %topicName, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadata, partitionMetadata = request)
    serializeSimpleCommand command

let newSend (producerId : ProducerId) (sequenceId : SequenceId) (highestSequenceId: SequenceId option)
    (numMessages : int) (msgMetadata : MessageMetadata) (payload: byte[]) : Payload =
    let request = CommandSend(ProducerId = %producerId, SequenceId = uint64 %sequenceId)
    if numMessages > 1 then
        request.NumMessages <- numMessages
    if highestSequenceId.IsSome then
        request.HighestSequenceId <- uint64 %highestSequenceId.Value
    if msgMetadata.TotalChunkMsgSize > 0 then
        request.IsChunk <- true
    if msgMetadata.ShouldSerializeTxnidLeastBits() then
        request.TxnidLeastBits <- msgMetadata.TxnidLeastBits
    if msgMetadata.ShouldSerializeTxnidMostBits() then
        request.TxnidMostBits <- msgMetadata.TxnidMostBits
    let command = BaseCommand(``type`` = CommandType.Send, Send = request)
    serializePayloadCommand command msgMetadata payload

let newAck (consumerId : ConsumerId) (ledgerId: LedgerId) (entryId: EntryId) (ackType : AckType)
    (properties: IReadOnlyDictionary<string, int64>) (ackSet: AckSet) (validationError: CommandAck.ValidationError option)
    (txnId: TxnId option) (requestId: RequestId option) (batchSize: int option) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = ackType.ToCommandAckType())
    let messageIdData = MessageIdData(ledgerId = uint64 %ledgerId,
                                         entryId = uint64 %entryId,
                                         AckSets = ackSet)
    match batchSize with
    | Some batchSize -> messageIdData.BatchSize <- batchSize
    | None -> ()
    request.MessageIds.Add(messageIdData)
    match validationError with
    | Some validationError -> request.validation_error <- validationError
    | None -> ()
    match txnId with
    | Some txnId ->
        request.TxnidMostBits <- txnId.MostSigBits
        request.TxnidLeastBits <- txnId.LeastSigBits
    | None -> ()
    match requestId with
    | Some requestId -> request.RequestId <- %requestId
    | None -> ()
    properties
    |> Seq.map(fun (KeyValue(key, value)) -> KeyLongValue(Key = key, Value = uint64 value))
    |> request.Properties.AddRange
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newMultiMessageAck (consumerId : ConsumerId) (messages: seq<LedgerId*EntryId*AckSet>) : Payload =
    let request = CommandAck(ConsumerId = %consumerId, ack_type = CommandAck.AckType.Individual)
    messages
    |> Seq.map (fun (ledgerId, entryId, ackSet) ->
            MessageIdData(ledgerId = uint64 %ledgerId, entryId = uint64 %entryId, AckSets = ackSet)
        )
    |> request.MessageIds.AddRange
    let command = BaseCommand(``type`` = CommandType.Ack, Ack = request)
    serializeSimpleCommand command

let newConnect (authMethodName: string) (authData: AuthData) (clientVersion: string) (protocolVersion: ProtocolVersion) (proxyToBroker: Option<DnsEndPoint>) : Payload =
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = int protocolVersion, AuthMethodName = authMethodName)
    if authMethodName = "ycav1" then
        request.AuthMethod <- AuthMethod.AuthMethodYcaV1
    if authData.Bytes.Length > 0 then
        request.AuthData <- authData.Bytes
    match proxyToBroker with
    | Some logicalAddress -> request.ProxyToBrokerUrl <- sprintf "%s:%d" logicalAddress.Host logicalAddress.Port
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.Connect, Connect = request)
    command |> serializeSimpleCommand

let newPing () : Payload =
    let request = CommandPing()
    let command = BaseCommand(``type`` = CommandType.Ping, Ping = request)
    command |> serializeSimpleCommand

let newPong () : Payload =
    let request = CommandPong()
    let command = BaseCommand(``type`` = CommandType.Pong, Pong = request)
    command |> serializeSimpleCommand

let newLookup (topicName : CompleteTopicName) (requestId : RequestId) (authoritative : bool) (listenerName: string) =
    let request = CommandLookupTopic(Topic = %topicName, Authoritative = authoritative, RequestId = uint64(%requestId))
    if listenerName |> String.IsNullOrEmpty |> not then
        request.AdvertisedListenerName <- listenerName
    let command = BaseCommand(``type`` = CommandType.Lookup, lookupTopic = request)
    command |> serializeSimpleCommand

let newProducer (topicName : CompleteTopicName) (producerName: string) (producerId : ProducerId) (requestId : RequestId)
                (schemaInfo: SchemaInfo) (epoch: uint64) (txnEnabled: bool) =
    let schema = getProtoSchema schemaInfo
    let request = CommandProducer(Topic = %topicName, ProducerId = %producerId, RequestId = %requestId,
                                  Epoch = epoch, TxnEnabled = txnEnabled)
    if producerName |> String.IsNullOrEmpty |> not then
        request.ProducerName <- producerName
    if schema.``type`` <> Schema.Type.None then
        request.Schema <- schema
    let command = BaseCommand(``type`` = CommandType.Producer, Producer = request)
    command |> serializeSimpleCommand

let newSeekByMsgId (consumerId: ConsumerId) (requestId : RequestId) (messageId: MessageId) =
    let request =
        CommandSeek(
            ConsumerId = %consumerId, RequestId = %requestId,
            MessageId = MessageIdData(
                ledgerId = uint64(%messageId.LedgerId),
                entryId = uint64(%messageId.EntryId)
            )
        )
    match messageId.Type with
    | Batch (batchIndex, acker) ->
        let batchSize =
            if acker = BatchMessageAcker.NullAcker then
                0
            else
                acker.GetBatchSize()
        let ackSet = BitArray batchSize
        for i in %batchIndex..batchSize-1 do
            ackSet.Set(i, true)
        request.MessageId.AckSets <- (ackSet |> toLongArray)
    | _ ->
        ()
    let command = BaseCommand(``type`` = CommandType.Seek, Seek = request)
    command |> serializeSimpleCommand

let newSeekByTimestamp (consumerId: ConsumerId) (requestId : RequestId) (timestamp: TimeStamp) =
    let request =
        CommandSeek(
            ConsumerId = %consumerId, RequestId = %requestId, MessagePublishTime = uint64 timestamp
        )
    let command = BaseCommand(``type`` = CommandType.Seek, Seek = request)
    command |> serializeSimpleCommand

let newGetTopicsOfNamespaceRequest (ns : NamespaceName) (requestId : RequestId) (isPersistent : bool) =
    let mode =
        match isPersistent with
        | true -> CommandGetTopicsOfNamespace.Mode.Persistent
        | false -> CommandGetTopicsOfNamespace.Mode.NonPersistent
    let request = CommandGetTopicsOfNamespace(Namespace = ns.ToString(), RequestId = uint64(%requestId), mode = mode)
    let command = BaseCommand(``type`` = CommandType.GetTopicsOfNamespace, getTopicsOfNamespace = request)
    command |> serializeSimpleCommand

let newSubscribe (topicName: CompleteTopicName) (subscription: SubscriptionName) (consumerId: ConsumerId) (requestId: RequestId)
    (consumerName: string) (subscriptionType: SubscriptionType) (subscriptionInitialPosition: SubscriptionInitialPosition)
    (readCompacted: bool) (startMessageId: MessageIdData) (durable: bool) (startMessageRollbackDuration: TimeSpan)
    (createTopicIfDoesNotExist: bool) (keySharedPolicy: KeySharedPolicy option) (schemaInfo: SchemaInfo) (priorityLevel: PriorityLevel)
    (replicateSubscriptionState: bool)=
    let schema = getProtoSchema schemaInfo
    let subType =
        match subscriptionType with
        | SubscriptionType.Exclusive -> CommandSubscribe.SubType.Exclusive
        | SubscriptionType.Shared -> CommandSubscribe.SubType.Shared
        | SubscriptionType.Failover -> CommandSubscribe.SubType.Failover
        | SubscriptionType.KeyShared -> CommandSubscribe.SubType.KeyShared
        | _ -> failwith "Unknown subscription type"
    let initialPosition =
        match subscriptionInitialPosition with
        | SubscriptionInitialPosition.Earliest -> CommandSubscribe.InitialPosition.Earliest
        | SubscriptionInitialPosition.Latest -> CommandSubscribe.InitialPosition.Latest
        | _ -> failwith "Unknown initialPosition type"
    let request = CommandSubscribe(Topic = %topicName, Subscription = %subscription, subType = subType, ConsumerId = %consumerId,
                    ConsumerName = consumerName, RequestId = %requestId, initialPosition = initialPosition, ReadCompacted = readCompacted,
                    StartMessageId = startMessageId, Durable = durable, ForceTopicCreation = createTopicIfDoesNotExist, PriorityLevel = %priorityLevel,
                    ReplicateSubscriptionState = replicateSubscriptionState)
    match keySharedPolicy with
    | Some keySharedPolicy ->
        let meta = KeySharedMeta()
        meta.allowOutOfOrderDelivery <- keySharedPolicy.AllowOutOfOrderDelivery
        match keySharedPolicy with
        | :? KeySharedPolicyAutoSplit ->
            meta.keySharedMode <- KeySharedMode.AutoSplit
        | :? KeySharedPolicySticky as policy ->
            meta.keySharedMode <- KeySharedMode.Sticky
            for range in policy.Ranges do
                meta.hashRanges.Add(IntRange(Start = range.Start, End = range.End))
        | _ -> failwith "Unknown keySharedPolicy"
        request.keySharedMeta <- meta
    | None ->
        ()
    if startMessageRollbackDuration > TimeSpan.Zero then
        request.StartMessageRollbackDurationSec <- (startMessageRollbackDuration.TotalSeconds |> uint64)
    if schema.``type`` <> Schema.Type.None then
        request.Schema <- schema
    let command = BaseCommand(``type`` = CommandType.Subscribe, Subscribe = request)
    command |> serializeSimpleCommand

let newFlow (consumerId: ConsumerId) (messagePermits: int) =
    let request = CommandFlow(ConsumerId = %consumerId, messagePermits = (uint32 messagePermits))
    let command = BaseCommand(``type`` = CommandType.Flow, Flow = request)
    command |> serializeSimpleCommand

let newCloseConsumer (consumerId: ConsumerId) (requestId : RequestId) =
    let request = CommandCloseConsumer(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.CloseConsumer, CloseConsumer = request)
    command |> serializeSimpleCommand

let newUnsubscribeConsumer (consumerId: ConsumerId) (requestId : RequestId) =
    let request = CommandUnsubscribe(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.Unsubscribe, Unsubscribe = request)
    command |> serializeSimpleCommand

let newCloseProducer (producerId: ProducerId) (requestId : RequestId) =
    let request = CommandCloseProducer(ProducerId = %producerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.CloseProducer, CloseProducer = request)
    command |> serializeSimpleCommand

let newRedeliverUnacknowledgedMessages (consumerId: ConsumerId) (messageIds : Option<MessageIdData[]>) =
    let request = CommandRedeliverUnacknowledgedMessages(ConsumerId = %consumerId)
    match messageIds with
    | Some ids -> ids |> Array.iter request.MessageIds.Add
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.RedeliverUnacknowledgedMessages, redeliverUnacknowledgedMessages = request)
    command |> serializeSimpleCommand

let newGetLastMessageId (consumerId: ConsumerId) (requestId: RequestId) =
    let request = CommandGetLastMessageId(ConsumerId = %consumerId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.GetLastMessageId, getLastMessageId = request)
    command |> serializeSimpleCommand

let newGetSchema (topicName: CompleteTopicName) (requestId : RequestId) (schemaVersion: Option<SchemaVersion>) =
    let request = CommandGetSchema(Topic = %topicName, RequestId = %requestId)
    match schemaVersion with
    | Some sv -> request.SchemaVersion <- sv.Bytes
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.GetSchema, getSchema = request)
    command |> serializeSimpleCommand

let newTxn (tcId: TransactionCoordinatorId) (requestId: RequestId) (ttl: TimeSpan) =
    let request = CommandNewTxn(TcId = %tcId, RequestId = %requestId, TxnTtlSeconds = uint64 ttl.TotalMilliseconds)
    let command = BaseCommand(``type`` = CommandType.NewTxn, newTxn = request)
    command |> serializeSimpleCommand

let newAddPartitionToTxn (txn: TxnId) (requestId: RequestId) (partition: CompleteTopicName) =
    let request = CommandAddPartitionToTxn(TxnidLeastBits = txn.LeastSigBits, TxnidMostBits = txn.MostSigBits, RequestId = %requestId)
    request.Partitions.Add(%partition)
    let command = BaseCommand(``type`` = CommandType.AddPartitionToTxn, addPartitionToTxn = request)
    command |> serializeSimpleCommand

let newAddSubscriptionToTxn (txn: TxnId) (requestId: RequestId) (topic: CompleteTopicName) (subscription: SubscriptionName) =
    let request = CommandAddSubscriptionToTxn(TxnidLeastBits = txn.LeastSigBits, TxnidMostBits = txn.MostSigBits, RequestId = %requestId)
    request.Subscriptions.Add(Subscription(Topic = %topic, subscription = %subscription))
    let command = BaseCommand(``type`` = CommandType.AddSubscriptionToTxn, addSubscriptionToTxn = request)
    command |> serializeSimpleCommand

let newEndTxn (txn: TxnId) (requestId: RequestId)  (action: TxnAction)  =
    let request = CommandEndTxn(TxnidLeastBits = txn.LeastSigBits, TxnidMostBits = txn.MostSigBits, RequestId = %requestId,
                                TxnAction = action)
    let command = BaseCommand(``type`` = CommandType.EndTxn, endTxn = request)
    command |> serializeSimpleCommand

let newAuthResponse (authMethod: string) (clientData: AuthData) (protocolVersion: int) (clientVersion: string) =
    let response = pulsar.proto.AuthData(AuthMethodName = authMethod, auth_data = clientData.Bytes)
    let request = CommandAuthResponse(ClientVersion = clientVersion, Response = response, ProtocolVersion = protocolVersion)
    let command = BaseCommand(``type`` = CommandType.AuthResponse, authResponse = request)
    command |> serializeSimpleCommand

let newTcClientConnectRequest (transactionCoordinatorId: TransactionCoordinatorId) (requestId: RequestId) =
    let request = CommandTcClientConnectRequest(TcId = %transactionCoordinatorId, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.TcClientConnectRequest, tcClientConnectRequest = request)
    command |> serializeSimpleCommand