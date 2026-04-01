namespace Pulsar.Client.Api

open System.Threading

open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.Schema
open System
open Microsoft.Extensions.Logging
open System.Collections.Generic
open System.Timers
open System.IO
open System.Runtime.InteropServices
open Pulsar.Client.Transaction
open System.Threading.Channels

type SendMessageRequest<'T> = MessageBuilder<'T> * TaskCompletionSource<MessageId> * bool

type internal ProducerTickType =
    | SendBatchTick
    | SendTimeoutTick
    | StatTick
    | UpdateEncryptionKeys of IMessageEncryptor

type internal ProducerMessage<'T> =
    | ConnectionOpened of uint64
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | AckReceived of SendReceipt
    | BeginSendMessage of SendMessageRequest<'T>
    | RecoverChecksumError of SequenceId
    | RecoverNotAllowedError of SequenceId
    | TopicTerminatedError
    | Close of TaskCompletionSource<ResultOrException<unit>>
    | Tick of ProducerTickType
    | GetStats of TaskCompletionSource<ProducerStats>

type internal ProducerImpl<'T> private (producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                           partitionIndex: int, lookup: ILookupService, schema: ISchema<'T>,
                           interceptors: ProducerInterceptors<'T>, cleanup: ProducerImpl<'T> -> unit) as this =
    let _this = this :> IProducer<'T>
    let producerId = Generators.getNextProducerId()
    let mutable producerName = producerConfig.ProducerName
    let mutable prefix = $"producer({producerId}, {producerName}, {partitionIndex})"
    let producerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE
    let mutable schemaVersion: SchemaVersion option = None
    let pendingMessages = Queue<PendingMessage<'T>>()
    let compressionCodec = CompressionCodec.get producerConfig.CompressionType
    let keyValueProcessor = KeyValueProcessor.GetInstance schema
    let blockedRequests = Queue<SendMessageRequest<'T>>()

    let initialSequenceId: SequenceId = producerConfig.InitialSequenceId |> Option.defaultValue %(-1L)
    let mutable lastSequenceIdPublished = %initialSequenceId
    let mutable lastSequenceIdPushed = initialSequenceId
    let mutable msgIdGenerator = initialSequenceId + %1L
    let mutable isLastSequenceIdPotentialDuplicated = false

    let protoCompressionType =
        match producerConfig.CompressionType with
        | CompressionType.None -> pulsar.proto.CompressionType.None
        | CompressionType.ZLib -> pulsar.proto.CompressionType.Zlib
        | CompressionType.LZ4 -> pulsar.proto.CompressionType.Lz4
        | CompressionType.ZStd -> pulsar.proto.CompressionType.Zstd
        | CompressionType.Snappy -> pulsar.proto.CompressionType.Snappy
        | _ -> pulsar.proto.CompressionType.None

    let createProducerTimeout = DateTime.Now.Add(clientConfig.OperationTimeout)
    let sendTimeoutMs = producerConfig.SendTimeout.TotalMilliseconds
    let stats =
        if clientConfig.StatsInterval = TimeSpan.Zero then
            ProducerStatsImpl.PRODUCER_STATS_DISABLED
        else
            ProducerStatsImpl(prefix) :> IProducerStatsRecorder
    let connectionHandler =
        ConnectionHandler(prefix,
                          connectionPool,
                          lookup,
                          producerConfig.Topic.CompleteTopicName,
                          (fun epoch -> post this.Mb (ProducerMessage.ConnectionOpened epoch)),
                          (fun ex -> post this.Mb (ProducerMessage.ConnectionFailed ex)),
                          Backoff { BackoffConfig.Default with
                                        Initial = clientConfig.InitialBackoffInterval
                                        Max = clientConfig.MaxBackoffInterval
                                        MandatoryStop = TimeSpan.FromMilliseconds(Math.Max(100.0, sendTimeoutMs - 100.0))})

    let batchMessageContainer =
        match producerConfig.BatchBuilder with
        | BatchBuilder.Default ->
            DefaultBatchMessageContainer(prefix, producerConfig) :> MessageContainer<'T>
        | BatchBuilder.KeyBased ->
            KeyBasedBatchMessageContainer(prefix, producerConfig) :> MessageContainer<'T>
        | _ -> failwith "Unknown BatchBuilder type"

    let failMessage (message: MessageBuilder<'T>) (tcs: TaskCompletionSource<MessageId> option) (ex: exn) =
        interceptors.OnSendAcknowledgement(this, message, Unchecked.defaultof<MessageId>, ex)
        stats.IncrementSendFailed()
        tcs |> Option.iter (fun tcs -> tcs.SetException(ex))

    let failPendingMessage msg (ex: exn) =
        match msg.Callback with
        | SingleCallback (chunkDetails, message, tcs) ->
            if chunkDetails.IsNone || chunkDetails.Value.IsLast then
                failMessage message tcs ex
        | BatchCallbacks tcss ->
            tcss
            |> Seq.iter (fun (_, message, tcs) ->
                failMessage message tcs ex)

    let failPendingBatchMessages (ex: exn) =
        if batchMessageContainer.NumMessagesInBatch > 0 then
            batchMessageContainer.Discard ex

    let failPendingMessages (ex: exn) =
        while pendingMessages.Count > 0 do
            let msg = pendingMessages.Dequeue()
            failPendingMessage msg ex
        while blockedRequests.Count > 0 do
            let _, channel, _ = blockedRequests.Dequeue()
            channel.SetException ex
        if producerConfig.BatchingEnabled then
            failPendingBatchMessages ex

    let sendTimeoutTimer = new Timer()
    let startSendTimeoutTimer () =
        if sendTimeoutMs > 0.0 then
            sendTimeoutTimer.Interval <- sendTimeoutMs
            sendTimeoutTimer.AutoReset <- true
            sendTimeoutTimer.Elapsed.Add(fun _ -> post this.Mb (Tick SendTimeoutTick))
            sendTimeoutTimer.Start()

    let batchTimer = new Timer()
    let startSendBatchTimer () =
        batchTimer.Interval <- producerConfig.BatchingMaxPublishDelay.TotalMilliseconds
        batchTimer.AutoReset <- true
        batchTimer.Elapsed.Add(fun _ -> post this.Mb (Tick SendBatchTick))
        batchTimer.Start()

    let statTimer = new Timer()
    let startStatTimer () =
        if clientConfig.StatsInterval <> TimeSpan.Zero then
            statTimer.Interval <- clientConfig.StatsInterval.TotalMilliseconds
            statTimer.AutoReset <- true
            statTimer.Elapsed.Add(fun _ -> post this.Mb (Tick StatTick))
            statTimer.Start()

    let cryptoTimer = new Timer()
    let startCryptoTimer () =
        match producerConfig.MessageEncryptor with
        | Some encryptor ->
            cryptoTimer.Interval <- TimeSpan(hours = 4, minutes = 0, seconds = 0).TotalMilliseconds
            cryptoTimer.AutoReset <- true
            cryptoTimer.Elapsed.Add(fun _ -> post this.Mb (Tick (UpdateEncryptionKeys encryptor)))
            cryptoTimer.Start()
        | None -> ()

    let encrypt =
        match producerConfig.MessageEncryptor with
        | Some msgCrypto ->
            fun (msgMetadata: MessageMetadata) (payload: byte []) ->
                try
                    let encMsg = msgCrypto.Encrypt(payload)
                    encMsg.EncryptionKeys |> Array.iter (EncryptionKey.ToProto >> msgMetadata.EncryptionKeys.Add)
                    msgMetadata.EncryptionAlgo <- encMsg.EncryptionAlgo
                    msgMetadata.EncryptionParam <- encMsg.EncryptionParam
                    Ok encMsg.EncPayload
                with ex ->
                    match producerConfig.ProducerCryptoFailureAction with
                    | ProducerCryptoFailureAction.SEND ->
                        Log.Logger.LogWarning(ex,
                            "{0} Failed to encrypt message. Proceeding with publishing unencrypted message", prefix)
                        Ok payload
                    | ProducerCryptoFailureAction.FAIL ->
                        Log.Logger.LogError(ex, "{0} Producer cannot encrypt message, failing", prefix)
                        Error ex
                    | _ -> failwith "Unknown ProducerCryptoFailureAction"
        | None ->
            Log.Logger.LogDebug("{0} CryptoKeyReader not present, encryption not possible", prefix)
            fun _ payload ->
                Ok payload

    let sendMessage (pendingMessage: PendingMessage<'T>) =
        Log.Logger.LogDebug("{0} sendMessage sequenceId={1}", prefix, %pendingMessage.SequenceId)
        pendingMessages.Enqueue(pendingMessage)
        match connectionHandler.ConnectionState with
        | Ready clientCnx ->
            clientCnx.SendAndForget pendingMessage.Payload
        | _ ->
            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)

    let dequeuePendingMessage (pendingMessage: PendingMessage<'T>) =
        if blockedRequests.Count > 0 then
            let messagesToRelease =
                match pendingMessage.Callback with
                | SingleCallback _ -> 1
                | BatchCallbacks callbacks -> callbacks.Length
            let mutable i = messagesToRelease
            while i > 0 && blockedRequests.Count > 0 do
                blockedRequests.Dequeue()
                |> BeginSendMessage
                |> post this.Mb
                i <- i - 1
        pendingMessages.Dequeue() |> ignore

    let resendMessages (clientCnx: ClientCnx) =
        if pendingMessages.Count > 0 then
            Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
            for pendingMessage in pendingMessages do
                clientCnx.SendAndForget pendingMessage.Payload
        else
            Log.Logger.LogDebug("{0} No pending messages to resend", prefix)
            producerCreatedTsc.TrySetResult() |> ignore

    let verifyIfLocalBufferIsCorrupted (msg: PendingMessage<'T>) =
        backgroundTask {
            use stream = MemoryStreamManager.GetStream()
            use reader = new BinaryReader(stream)
            do! (fst msg.Payload) (stream :> Stream) // materialize stream
            let streamSize = stream.Length
            stream.Seek(4L, SeekOrigin.Begin) |> ignore
            let cmdSize = reader.ReadInt32() |> int32FromBigEndian
            stream.Seek((10+cmdSize) |> int64, SeekOrigin.Begin) |> ignore
            let checkSum = reader.ReadInt32() |> int32FromBigEndian
            let checkSumPayload = (int streamSize) - 14 - cmdSize
            let computedCheckSum = CRC32C.Get(0u, stream, checkSumPayload) |> int32
            return checkSum <> computedCheckSum
        }

    let canAddToBatch (message : MessageBuilder<'T>) =
        producerConfig.BatchingEnabled && message.DeliverAt.IsNone

    let createMessageMetadata (sequenceId: SequenceId) (txnId: TxnId option) (numMessagesInBatch: int option)
        (payload: byte[]) (key: MessageKey option) (properties: IReadOnlyDictionary<string, string>) (deliverAt: TimeStamp option)
        (orderingKey: byte[] option) (eventTime: TimeStamp option) (replicationClusters: IEnumerable<string> option) =
        let metadata =
            MessageMetadata (
                SequenceId = (sequenceId |> uint64),
                PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() |> uint64),
                ProducerName = producerConfig.ProducerName,
                UncompressedSize = (payload.Length |> uint32)
            )
        if payload.Length = 0 then
            metadata.NullValue <- true
        if protoCompressionType <> pulsar.proto.CompressionType.None then
            metadata.Compression <- protoCompressionType
        match key with
        | Some key ->
            metadata.PartitionKey <- %key.PartitionKey
            metadata.PartitionKeyB64Encoded <- key.IsBase64Encoded
        | None ->
            metadata.NullPartitionKey <- true
        if properties.Count > 0 then
            for property in properties do
                metadata.Properties.Add(KeyValue(Key = property.Key, Value = property.Value))
        if numMessagesInBatch.IsSome then
            metadata.NumMessagesInBatch <- numMessagesInBatch.Value
        match deliverAt with
        | Some deliverAt ->
            metadata.DeliverAtTime <- %deliverAt
        | None ->
            ()
        match schemaVersion with
        | Some sv ->
            metadata.SchemaVersion <- sv.Bytes
        | None ->
            ()
        match orderingKey with
        | Some orderingKey ->
            metadata.OrderingKey <- orderingKey
        | None ->
            ()
        match eventTime with
        | Some eventTime ->
            metadata.EventTime <- eventTime |> uint64
        | None ->
            ()
        match txnId with
        | Some txnId ->
            metadata.TxnidLeastBits <- txnId.LeastSigBits
            metadata.TxnidMostBits <- txnId.MostSigBits
        | None ->
            ()
        match replicationClusters with
        | Some replicationClusters ->
            metadata.ReplicateToes.AddRange(replicationClusters)
        | None ->
            ()

        metadata

    let getHighestSequenceId (pendingMessage: PendingMessage<'T>): SequenceId =
        %Math.Max(%pendingMessage.SequenceId, %pendingMessage.HighestSequenceId)

    let processOpSendMsg { OpSendMsg = opSendMsg; LowestSequenceId = lowestSequenceId; HighestSequenceId = highestSequenceId;
                          PartitionKey = partitionKey; OrderingKey = orderingKey; TxnId = txnId; ReplicationClusters = replicationClusters } =
        let batchPayload, batchCallbacks = opSendMsg
        let batchSize = batchCallbacks.Length
        let metadata = createMessageMetadata lowestSequenceId txnId (Some batchSize)
                           batchPayload partitionKey EmptyProps None orderingKey None replicationClusters
        let compressedBatchPayload = compressionCodec.Encode batchPayload
        if (compressedBatchPayload.Length > maxMessageSize) then
            batchCallbacks
            |> Seq.iter (fun (_, message, tcs) ->
                let ex = InvalidMessageException <| $"Message size is bigger than {maxMessageSize} bytes"
                failMessage message tcs ex)
        else
            let encryptResult = encrypt metadata compressedBatchPayload
            match encryptResult with
            | Ok encryptedBatchPayload ->
                stats.UpdateNumMsgsSent(batchSize, compressedBatchPayload.Length)
                let payload = Commands.newSend producerId lowestSequenceId (Some highestSequenceId) batchSize metadata encryptedBatchPayload
                let pendingMessage = {
                    SequenceId = lowestSequenceId
                    HighestSequenceId = highestSequenceId
                    Payload = payload
                    Callback = BatchCallbacks batchCallbacks
                    CreatedAt = DateTime.Now
                }
                lastSequenceIdPushed <- %Math.Max(%lastSequenceIdPushed, %(getHighestSequenceId pendingMessage))
                sendMessage pendingMessage
            | Error ex ->
                batchCallbacks
                |> Seq.iter (fun (_, message, tcs) ->
                    failMessage message tcs ex)

    let canAddToCurrentBatch (msg: MessageBuilder<'T>) =
        batchMessageContainer.HaveEnoughSpace(msg) && batchMessageContainer.HasSameTxn(msg)

    let batchMessageAndSend() =
        let batchSize = batchMessageContainer.NumMessagesInBatch
        Log.Logger.LogTrace("{0} Batching the messages from the batch container with {1} messages", prefix, batchSize)
        if batchSize > 0 then
            if batchMessageContainer.IsMultiBatches then
                batchMessageContainer.CreateOpSendMsgs()
                |> Seq.iter processOpSendMsg
            else
                batchMessageContainer.CreateOpSendMsg() |> processOpSendMsg
        batchMessageContainer.Clear()

    let doBatchSendAndAdd batchItem =
        Log.Logger.LogDebug("{0} Closing out batch to accommodate large message with size {1}", prefix, batchItem.Message.Payload.Length)
        batchMessageAndSend()
        batchMessageContainer.Add(batchItem) |> ignore

    let updateMaxMessageSize messageSize =
        maxMessageSize <- messageSize
        batchMessageContainer.MaxMessageSize <- messageSize

    let canEnqueueRequest (channel: TaskCompletionSource<MessageId>) sendRequest msgCount =
        if pendingMessages.Count + msgCount > producerConfig.MaxPendingMessages then
            if producerConfig.BlockIfQueueFull then
                blockedRequests.Enqueue(sendRequest)
            else
                channel.SetException(ProducerQueueIsFullError "Producer send queue is full")
            false
        else
            true

    let beginSendMessage (sendRequest: SendMessageRequest<'T>) =
        let message, channel, isFireAndForget = sendRequest
        if canEnqueueRequest channel sendRequest 1 then
            if isFireAndForget then
                channel.SetResult <| Unchecked.defaultof<MessageId>
            let channel =
                if isFireAndForget then
                    None
                else
                    Some channel
            let sequenceId =
                match message.SequenceId with
                | Some seqId ->
                    seqId
                | None ->
                    let oldValue = msgIdGenerator
                    msgIdGenerator <- msgIdGenerator + %1L
                    %oldValue
            if canAddToBatch message then
                let batchItem = { Message = message; Tcs = channel; SequenceId = sequenceId }
                if canAddToCurrentBatch message then
                    // should trigger complete the batch message, new message will add to a new batch and new batch
                    // sequence id use the new message, so that broker can handle the message duplication
                    if sequenceId <= lastSequenceIdPushed then
                        isLastSequenceIdPotentialDuplicated <- true
                        if sequenceId <= %lastSequenceIdPublished then
                            Log.Logger.LogWarning("{0} Message with sequence id {1} is definitely a duplicate",
                                                  prefix, message.SequenceId)
                        else
                            Log.Logger.LogInformation("{0} Message with sequence id {1} might be a duplicate but cannot be determined at this time.",
                                                      prefix, message.SequenceId)
                        doBatchSendAndAdd batchItem
                    else
                        // Should flush the last potential duplicated since can't combine potential duplicated messages
                        // and non-duplicated messages into a batch.
                        if isLastSequenceIdPotentialDuplicated then
                            doBatchSendAndAdd batchItem
                        else
                            // handle boundary cases where message being added would exceed
                            // batch size and/or max message size
                            if batchMessageContainer.Add(batchItem) then
                                Log.Logger.LogDebug("{0} Max batch container size exceeded", prefix)
                                batchMessageAndSend()
                        isLastSequenceIdPotentialDuplicated <- false
                else
                   doBatchSendAndAdd batchItem
            else
                let compressedPayload = compressionCodec.Encode message.Payload
                if compressedPayload.Length > maxMessageSize && not producerConfig.ChunkingEnabled then
                    let ex = InvalidMessageException <| $"Message size is bigger than {maxMessageSize} bytes"
                    failMessage message channel ex
                else
                    let totalChunks =
                        Math.Max(1, compressedPayload.Length) / maxMessageSize
                            + (if Math.Max(1, compressedPayload.Length) % maxMessageSize = 0 then 0 else 1)
                    let mutable readStartIndex = 0
                    let mutable chunkError = false
                    let mutable chunkId = 0
                    let isChunked = totalChunks > 1
                    let uuid = if isChunked then $"{producerName}-{sequenceId}" else null
                    let messageIds = if isChunked then Array.zeroCreate totalChunks else Array.empty
                    let txnId = message.Txn |> Option.map (fun txn -> txn.Id)
                    while chunkId < totalChunks && not chunkError do
                        let metadata = createMessageMetadata sequenceId txnId None
                                           message.Payload message.Key message.Properties message.DeliverAt
                                           message.OrderingKey message.EventTime message.ReplicationClusters
                        let chunkPayload =
                            if isChunked && producerConfig.Topic.IsPersistent then
                                metadata.Uuid <- uuid
                                metadata.ChunkId <- chunkId
                                metadata.NumChunksFromMsg <- totalChunks
                                metadata.TotalChunkMsgSize <- compressedPayload.Length
                                Array.sub compressedPayload readStartIndex (Math.Min(maxMessageSize, compressedPayload.Length - readStartIndex))
                            else
                                compressedPayload
                        let encryptResult = encrypt metadata chunkPayload
                        match encryptResult with
                        | Ok encryptedPayload ->
                            stats.UpdateNumMsgsSent(1, chunkPayload.Length)
                            let payload = Commands.newSend producerId sequenceId None 1 metadata encryptedPayload
                            let chunkDetails =
                                if isChunked then
                                    Some { TotalChunks = totalChunks; ChunkId = chunkId; MessageIds = messageIds }
                                else
                                    None
                            let pendingMessage = {
                                SequenceId = sequenceId
                                HighestSequenceId = %(-1L)
                                Payload = payload
                                Callback = SingleCallback (chunkDetails, message, channel)
                                CreatedAt = DateTime.Now
                            }
                            lastSequenceIdPushed <- %Math.Max(%lastSequenceIdPushed, %sequenceId)
                            sendMessage pendingMessage
                            chunkId <- chunkId + 1
                            readStartIndex <- chunkId * maxMessageSize
                        | Error ex ->
                            chunkError <- true
                            failMessage message channel ex

    let stopProducer() =
        sendTimeoutTimer.Stop()
        batchTimer.Stop()
        connectionHandler.Close()
        interceptors.Close()
        statTimer.Stop()
        cleanup(this)
        Log.Logger.LogInformation("{0} stopped", prefix)

    let producerOperations = {
        AckReceived = fun sendReceipt -> post this.Mb (AckReceived sendReceipt)
        TopicTerminatedError = fun () -> post this.Mb TopicTerminatedError
        RecoverChecksumError = fun seqId -> post this.Mb (RecoverChecksumError seqId)
        RecoverNotAllowedError = fun seqId -> post this.Mb (RecoverNotAllowedError seqId)
        ConnectionClosed = fun clientCnx -> post this.Mb (ConnectionClosed clientCnx)
    }

    let mb = Channel.CreateUnbounded<ProducerMessage<'T>>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! mb.Reader.ReadAsync() with
            | ProducerMessage.ConnectionOpened epoch ->
                match connectionHandler.ConnectionState with
                | Ready clientCnx ->
                    Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                    updateMaxMessageSize clientCnx.MaxMessageSize
                    clientCnx.AddProducer(producerId, producerOperations)
                    let requestId = Generators.getNextRequestId()
                    try
                        let payload = Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName
                                            producerId requestId schema.SchemaInfo epoch clientConfig.EnableTransaction
                        let! response = clientCnx.SendAndWaitForReply requestId payload
                        let success = response |> PulsarResponseType.GetProducerSuccess
                        if String.IsNullOrEmpty producerName then
                            producerName <- success.GeneratedProducerName
                            prefix <- $"producer({producerId}, {producerName}, {partitionIndex})"
                        Log.Logger.LogInformation("{0} registered", prefix)
                        schemaVersion <- success.SchemaVersion
                        connectionHandler.ResetBackoff()

                        if msgIdGenerator = %0L && producerConfig.InitialSequenceId.IsNone then
                            // Only update sequence id generator if it wasn't already modified. That means we only want
                            // to update the id generator the first time the producer gets established, and ignore the
                            // sequence id sent by broker in subsequent producer reconnects
                            lastSequenceIdPublished <- %success.LastSequenceId
                            msgIdGenerator <- success.LastSequenceId + %1L

                        if producerConfig.BatchingEnabled then
                            startSendBatchTimer()

                        resendMessages clientCnx
                    with Flatten ex ->
                        clientCnx.RemoveProducer producerId
                        Log.Logger.LogError(ex, "{0} Failed to create", prefix)
                        match ex with
                        | :? TopicDoesNotExistException ->
                            match connectionHandler.ConnectionState with
                            | Failed ->
                                Log.Logger.LogWarning("{0} Topic doesn't exist exception {1}", prefix, ex.Message)
                                let! _ = postAndAsyncReply this.Mb ProducerMessage.Close
                                producerCreatedTsc.TrySetException(ex) |> ignore
                            | _ -> ()
                        | :? ProducerBlockedQuotaExceededException ->
                            Log.Logger.LogWarning("{0} Topic backlog quota exceeded. {1}", prefix, ex.Message)
                            failPendingMessages(ex)
                        | :? ProducerBlockedQuotaExceededError ->
                            Log.Logger.LogWarning("{0} is blocked on creation because backlog exceeded. {1}", prefix, ex.Message)
                        | _ ->
                            ()

                        match ex with
                        | :? TopicTerminatedException ->
                            Log.Logger.LogWarning("{0} is terminated. {1}", prefix, ex.Message)
                            connectionHandler.Terminate()
                            failPendingMessages(ex)
                            producerCreatedTsc.TrySetException(ex) |> ignore
                            stopProducer()
                        | _ when producerCreatedTsc.Task.IsCompleted
                                    || (PulsarClientException.isRetriableError ex && DateTime.Now < createProducerTimeout) ->
                            // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                            // still within the initial timeout budget and we are dealing with a retryable error
                            connectionHandler.ReconnectLater ex
                        | _ ->
                            connectionHandler.Failed()
                            producerCreatedTsc.SetException(ex)
                            stopProducer()
                | _ ->
                    Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)

                if connectionHandler.ConnectionState = Failed then
                    continueLoop <- false

            | ProducerMessage.ConnectionClosed clientCnx ->

                Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                connectionHandler.ConnectionClosed clientCnx
                clientCnx.RemoveProducer(producerId)

            | ProducerMessage.ConnectionFailed ex ->

                Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                if (DateTime.Now > createProducerTimeout && producerCreatedTsc.TrySetException(ex)) then
                    Log.Logger.LogInformation("{0} creation failed", prefix)
                    connectionHandler.Failed()
                    stopProducer()
                    continueLoop <- false

            | ProducerMessage.BeginSendMessage sendRequest ->

                Log.Logger.LogDebug("{0} BeginSendMessage", prefix)
                beginSendMessage sendRequest

            | ProducerMessage.AckReceived receipt ->

                let sequenceId = receipt.SequenceId
                let highestSequenceId = receipt.HighestSequenceId
                if pendingMessages.Count > 0 then
                    let pendingMessage = pendingMessages.Peek()
                    let exptectedHighestSequenceId = int64 pendingMessage.HighestSequenceId
                    let expectedSequenceId = int64 pendingMessage.SequenceId
                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning("{0} Got ack for msg. expecting: {1} - {2} - got: {3} - {4} - queue-size: {5}",
                            prefix, pendingMessage.SequenceId, pendingMessage.HighestSequenceId,
                            receipt.SequenceId, receipt.HighestSequenceId, pendingMessages.Count)
                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Dispose()
                        | _ -> ()
                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogInformation("{0} Got ack for timed out msg. expecting: {1} - {2} - got: {3} - {4}",
                            prefix, pendingMessage.SequenceId, pendingMessage.HighestSequenceId,
                            receipt.SequenceId, receipt.HighestSequenceId)
                    else
                        // Add check `sequenceId >= highestSequenceId` for backward compatibility.
                        if sequenceId >= highestSequenceId || highestSequenceId = exptectedHighestSequenceId then
                            Log.Logger.LogDebug("{0} Received ack for message {1}", prefix, receipt)
                            dequeuePendingMessage pendingMessage
                            lastSequenceIdPublished <- Math.Max(lastSequenceIdPublished, %(getHighestSequenceId pendingMessage))
                            match pendingMessage.Callback with
                            | SingleCallback (chunkDetailsOption, msg, tcs) ->
                                if chunkDetailsOption.IsNone || chunkDetailsOption.Value.IsLast then
                                    let currentMessageId =
                                        { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex;
                                            Type = MessageIdType.Single; TopicName = %""; ChunkMessageIds = None }
                                    let msgId =
                                        match chunkDetailsOption with
                                        | Some chunkDetail ->
                                            chunkDetail.MessageIds.[chunkDetail.ChunkId] <- currentMessageId
                                            { currentMessageId with ChunkMessageIds = Some chunkDetail.MessageIds }
                                        | None ->
                                            currentMessageId
                                    interceptors.OnSendAcknowledgement(this, msg, msgId, null)
                                    stats.IncrementNumAcksReceived(DateTime.Now - pendingMessage.CreatedAt)
                                    tcs |> Option.iter (fun tcs -> tcs.SetResult(msgId))
                                else
                                    // updating messageIds array
                                    let chunkDetails = chunkDetailsOption.Value
                                    let currentMsgId =
                                                { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex;
                                                    Type = MessageIdType.Single; TopicName = %""; ChunkMessageIds = None }
                                    chunkDetails.MessageIds.[chunkDetails.ChunkId] <- currentMsgId

                            | BatchCallbacks tcss ->
                                tcss
                                |> Array.iter (fun (msgId, msg, tcs) ->
                                    let msgId = { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex;
                                                    Type = Batch msgId; TopicName = %""; ChunkMessageIds = None }
                                    interceptors.OnSendAcknowledgement(this, msg, msgId, null)
                                    stats.IncrementNumAcksReceived(DateTime.Now - pendingMessage.CreatedAt)
                                    tcs |> Option.iter (fun tcs -> tcs.SetResult(msgId)))
                        else
                            Log.Logger.LogWarning("{0} Got ack for batch msg error. expecting: {1} - {2} - got: {3} - {4} - queue-size: {5}",
                                                    prefix, pendingMessage.SequenceId, pendingMessage.HighestSequenceId,
                                                    receipt.SequenceId, receipt.HighestSequenceId, pendingMessages.Count)
                            // Force connection closing so that messages can be re-transmitted in a new connection
                            match connectionHandler.ConnectionState with
                            | Ready clientCnx -> clientCnx.Dispose()
                            | _ -> ()
                else
                    Log.Logger.LogInformation("{0} Got ack for timed out msg {1} - {2}", prefix, sequenceId, highestSequenceId)

            | ProducerMessage.RecoverChecksumError sequenceId ->

                //* Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
                //* message header-payload again.
                //* <ul>
                //* <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
                //* message</li>
                //* <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
                //* So, fail send-message by failing callback</li>
                //* </ul>
                Log.Logger.LogWarning("{0} RecoverChecksumError seqId={1}", prefix, sequenceId)
                if pendingMessages.Count > 0 then
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = getHighestSequenceId pendingMessage
                    if sequenceId = expectedSequenceId then
                        let! corrupted = verifyIfLocalBufferIsCorrupted pendingMessage
                        if corrupted then
                            // remove message from pendingMessages queue and fail callback
                            dequeuePendingMessage pendingMessage
                            failPendingMessage pendingMessage (ChecksumException "Checksum failed on corrupt message")
                        else
                            Log.Logger.LogDebug("{0} Message is not corrupted, retry send-message with sequenceId {1}", prefix, sequenceId)
                            match connectionHandler.ConnectionState with
                            | Ready clientCnx -> resendMessages clientCnx
                            | _ -> Log.Logger.LogWarning("{0} not connected, skipping send", prefix)

                    else
                        Log.Logger.LogDebug("{0} Corrupt message is already timed out {1}", prefix, sequenceId)
                else
                    Log.Logger.LogDebug("{0} Got send failure for timed out seqId {1}", prefix, sequenceId)

            | ProducerMessage.RecoverNotAllowedError sequenceId ->

                Log.Logger.LogWarning("{0} RecoverNotAllowedError seqId={1}", prefix, sequenceId)
                if pendingMessages.Count > 0 then
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = getHighestSequenceId pendingMessage
                    if sequenceId = expectedSequenceId then
                        failPendingMessage pendingMessage (
                            prefix + ": the size of the message is not allowed" |> NotAllowedException
                        )
                    else
                        Log.Logger.LogDebug("{0} Not allowed message is already timed out {1}", prefix, sequenceId)
                else
                    Log.Logger.LogDebug("{0} Got send failure for timed out seqId {1}", prefix, sequenceId)

            | ProducerMessage.TopicTerminatedError ->

                match connectionHandler.ConnectionState with
                | Closed | Terminated -> ()
                | _ ->
                    connectionHandler.Terminate()
                    failPendingMessages(TopicTerminatedException("The topic has been terminated"))

            | ProducerMessage.Tick tickType ->

                match tickType with
                | SendBatchTick ->
                    batchMessageAndSend()
                | SendTimeoutTick ->
                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        if pendingMessages.Count > 0 then
                            let firstMessage = pendingMessages.Peek()
                            if firstMessage.CreatedAt.AddMilliseconds(sendTimeoutMs) <= DateTime.Now then
                                // The diff is less than or equal to zero, meaning that the message has been timed out.
                                // Set the callback to timeout on every message, then clear the pending queue.
                                Log.Logger.LogInformation("{0} Message send timed out. Failing {1} messages", prefix, pendingMessages.Count)
                                let ex = TimeoutException "Could not send message to broker within given timeout"
                                failPendingMessages ex
                | StatTick ->
                    stats.TickTime(pendingMessages.Count)
                | UpdateEncryptionKeys encryptor ->
                    try
                        encryptor.UpdateEncryptionKeys()
                    with ex ->
                        Log.Logger.LogError(ex, "{0} Couldn't update encryption keys", prefix)


            | ProducerMessage.GetStats channel ->
                channel.SetResult <| stats.GetStats()

            | ProducerMessage.Close channel ->

                match connectionHandler.ConnectionState with
                | Ready clientCnx ->
                    connectionHandler.Closing()
                    Log.Logger.LogInformation("{0} starting close", prefix)
                    let requestId = Generators.getNextRequestId()
                    let payload = Commands.newCloseProducer producerId requestId
                    try
                        let! response = clientCnx.SendAndWaitForReply requestId payload
                        response |> PulsarResponseType.GetEmpty
                        clientCnx.RemoveProducer(producerId)
                        connectionHandler.Closed()
                        stopProducer()
                        failPendingMessages <| AlreadyClosedException "Producer was already closed"
                        channel.SetResult <| Ok()
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} failed to close", prefix)
                        channel.SetResult <| Error ex
                | _ ->
                    Log.Logger.LogInformation("{0} closing but current state {1}", prefix, connectionHandler.ConnectionState)
                    connectionHandler.Closed()
                    stopProducer()
                    failPendingMessages <| AlreadyClosedException "Producer was already closed"
                    channel.SetResult <| Ok()

                continueLoop <- false
            }:> Task).ContinueWith(fun t ->
                if t.IsFaulted then
                    let (Flatten ex) = t.Exception
                    Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
                else
                    Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    do startSendTimeoutTimer()
    do startStatTimer()
    do startCryptoTimer()

    member private this.SendMessage (message : MessageBuilder<'T>, isFireAndForget: bool): Task<MessageId> =
        backgroundTask {
            match message.Txn with
            | Some txn ->
                do! txn.RegisterProducedTopic(producerConfig.Topic.CompleteTopicName)
            | None ->
                ()
            let interceptMsg = interceptors.BeforeSend(this, message)
            return! postAndAsyncReply mb (fun channel -> BeginSendMessage (interceptMsg, channel, isFireAndForget))
        }

    member internal this.Mb with get(): Channel<ProducerMessage<'T>> = mb

    override this.Equals producer =
        producerId = (producer :?> IProducer<'T>).ProducerId

    override this.GetHashCode () = int producerId

    member private this.InitInternal() =
       backgroundTask {
           do connectionHandler.GrabCnx()
           return! producerCreatedTsc.Task
       }

    static member Init(producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                       partitionIndex: int, lookup: ILookupService, schema: ISchema<'T>,
                       interceptors: ProducerInterceptors<'T>, cleanup: ProducerImpl<'T> -> unit) =
        backgroundTask {
            let producer = ProducerImpl(producerConfig, clientConfig, connectionPool, partitionIndex, lookup, schema,
                                        interceptors, cleanup)
            do! producer.InitInternal()
            return producer
        }
    static member NewMessage<'T> (keyValueProcessor: IKeyValueProcessor option, schema: ISchema<'T>,
            value:'T,
            key:string,
            properties: IReadOnlyDictionary<string, string>,
            deliverAt:Nullable<TimeStamp>,
            sequenceId:Nullable<SequenceId>,
            keyBytes:byte[],
            orderingKey:byte[],
            eventTime: Nullable<TimeStamp>,
            txn:Transaction,
            replicationClusters: string seq) =
            keyValueProcessor
            |> Option.map(fun kvp -> kvp.EncodeKeyValue value)
            |> Option.map(fun struct(k, v) ->
                MessageBuilder(value, v,
                    (if String.IsNullOrEmpty(k) then None else Some { PartitionKey = %k; IsBase64Encoded = true }),
                    ?properties0 = (properties |> Option.ofObj),
                    ?deliverAt = (deliverAt |> Option.ofNullable),
                    ?sequenceId = (sequenceId |> Option.ofNullable),
                    ?orderingKey = (orderingKey |> Option.ofObj),
                    ?eventTime = (eventTime |> Option.ofNullable),
                    ?txn = (txn |> Option.ofObj),
                    ?replicationClusters = (replicationClusters |> Option.ofObj)))
            |> Option.defaultWith (fun () ->
                let keyObj =
                    if String.IsNullOrEmpty(key) && (isNull keyBytes || keyBytes.Length = 0) then
                        None
                    elif String.IsNullOrEmpty(key) |> not then
                        Some { PartitionKey = %key; IsBase64Encoded = false }
                    else
                        Some { PartitionKey = %Convert.ToBase64String(keyBytes); IsBase64Encoded = true }
                MessageBuilder(value, schema.Encode(value), keyObj,
                    ?properties0 = (properties |> Option.ofObj),
                    ?deliverAt = (deliverAt |> Option.ofNullable),
                    ?sequenceId = (sequenceId |> Option.ofNullable),
                    ?orderingKey = (orderingKey |> Option.ofObj),
                    ?eventTime = (eventTime |> Option.ofNullable),
                    ?txn = (txn |> Option.ofObj),
                    ?replicationClusters = (replicationClusters |> Option.ofObj)))

    interface IProducer<'T> with
        member this.SendAndForgetAsync (message: 'T) =
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let msg = _this.NewMessage message
            let sendTask = this.SendMessage(msg, true)
            if sendTask.IsFaulted then
                let (Flatten ex) = sendTask.Exception
                Task.FromException<Unit> ex
            else
                backgroundTask {
                    let! _ =  sendTask
                    return ()
                }

        member this.SendAndForgetAsync (message: MessageBuilder<'T>) =
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let sendTask = this.SendMessage(message, true)
            if sendTask.IsFaulted then
                let (Flatten ex) = sendTask.Exception
                Task.FromException<Unit> ex
            else
                message.Txn |> Option.iter (fun txn -> txn.RegisterSendOp(sendTask))
                backgroundTask {
                    let! _ =  sendTask
                    return ()
                }

        member this.SendAsync (message: 'T) =
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let msg = _this.NewMessage message
            this.SendMessage(msg, false)

        member this.SendAsync (message: MessageBuilder<'T>) =
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let sendTask = this.SendMessage(message, false)
            if not sendTask.IsFaulted then
                message.Txn |> Option.iter (fun txn -> txn.RegisterSendOp(sendTask))
            sendTask

        member this.NewMessage (value:'T,
            [<Optional; DefaultParameterValue(null:string)>]key:string,
            [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string, string>)>]properties: IReadOnlyDictionary<string, string>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<TimeStamp>)>]deliverAt:Nullable<TimeStamp>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<SequenceId>)>]sequenceId:Nullable<SequenceId>,
            [<Optional; DefaultParameterValue(null:byte[])>]keyBytes:byte[],
            [<Optional; DefaultParameterValue(null:byte[])>]orderingKey:byte[],
            [<Optional; DefaultParameterValue(Nullable():Nullable<TimeStamp>)>]eventTime:Nullable<TimeStamp>,
            [<Optional; DefaultParameterValue(null:Transaction)>]txn:Transaction,
            [<Optional; DefaultParameterValue(null:string seq)>]replicationClusters:string seq) =

            if (txn |> isNull |> not) && producerConfig.SendTimeout > TimeSpan.Zero then
                raise <| ArgumentException "Only producers disabled sendTimeout are allowed to produce transactional messages"

            ProducerImpl.NewMessage(keyValueProcessor, schema, value, key, properties,
                                    deliverAt, sequenceId, keyBytes, orderingKey, eventTime, txn, replicationClusters)

        member this.ProducerId = producerId

        member this.Topic = %producerConfig.Topic.CompleteTopicName

        member this.LastSequenceId = %Interlocked.Read(&lastSequenceIdPublished)

        member this.Name = producerName

        member this.GetStatsAsync() =
            postAndAsyncReply mb ProducerMessage.GetStats

        member this.LastDisconnectedTimestamp =
            connectionHandler.LastDisconnectedTimestamp
        member this.IsConnected =
            match connectionHandler.ConnectionState with
            | Ready _ -> true
            | _ -> false

    interface IAsyncDisposable with

        member this.DisposeAsync() =
            match connectionHandler.ConnectionState with
            | Closing | Closed ->
                ValueTask()
            | _ ->
                backgroundTask {
                    let! result = postAndAsyncReply mb ProducerMessage.Close
                    match result with
                    | Ok () -> ()
                    | Error ex -> reraize ex
                } |> ValueTask


