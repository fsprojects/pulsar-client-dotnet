namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
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

type internal ProducerMessage<'T> =
    | ConnectionOpened of uint64
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | AckReceived of SendReceipt
    | BeginSendMessage of MessageBuilder<'T> * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendMessage of PendingMessage<'T>
    | RecoverChecksumError of SequenceId
    | TopicTerminatedError
    | Close of AsyncReplyChannel<ResultOrException<unit>>
    | StoreBatchItem of MessageBuilder<'T> * AsyncReplyChannel<TaskCompletionSource<MessageId>>
    | SendBatchTick
    | SendTimeoutTick
    
type internal ProducerImpl<'T> private (producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                           partitionIndex: int, lookup: BinaryLookupService, schema: ISchema<'T>,
                           interceptors: ProducerInterceptors<'T>, cleanup: ProducerImpl<'T> -> unit) as this =
    let _this = this :> IProducer<'T>
    let producerId = Generators.getNextProducerId()    
    let prefix = sprintf "producer(%u, %s, %i)" %producerId producerConfig.ProducerName partitionIndex
    let producerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE
    let mutable schemaVersion = None
    let pendingMessages = Queue<PendingMessage<'T>>()
    let compressionCodec = CompressionCodec.create producerConfig.CompressionType
    let keyValueProcessor = KeyValueProcessor.GetInstance schema    

    let lastSequenceId = producerConfig.InitialSequenceId |> Option.defaultValue -1L
    let mutable lastSequenceIdPublished = lastSequenceId
    let lastSequenceIdPushed = lastSequenceId
    let mutable messageIdGenerator = lastSequenceId + 1L

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
    let connectionHandler =
        ConnectionHandler(prefix,
                          connectionPool,
                          lookup,
                          producerConfig.Topic.CompleteTopicName,
                          (fun epoch -> this.Mb.Post(ProducerMessage.ConnectionOpened epoch)),
                          (fun ex -> this.Mb.Post(ProducerMessage.ConnectionFailed ex)),
                          Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        Max = TimeSpan.FromSeconds(60.0)
                                        MandatoryStop = TimeSpan.FromMilliseconds(Math.Max(100.0, sendTimeoutMs - 100.0))})

    let batchMessageContainer =
        match producerConfig.BatchBuilder with
        | BatchBuilder.Default ->
            DefaultBatchMessageContainer(prefix, producerConfig) :> MessageContainer<'T>
        | BatchBuilder.KeyBased ->
            KeyBasedBatchMessageContainer(prefix, producerConfig) :> MessageContainer<'T>
        | _ -> failwith "Unknown BatchBuilder type"

    let failPendingMessage msg (ex: exn) =
        match msg.Callback with
        | SingleCallback (message, tcs) ->
            interceptors.OnSendAcknowledgement(this, message, Unchecked.defaultof<MessageId>, ex)
            tcs.SetException(ex)
        | BatchCallbacks tcss ->
            tcss
            |> Seq.iter (fun (_, message, tcs) ->
                interceptors.OnSendAcknowledgement(this, message, Unchecked.defaultof<MessageId>, ex)
                tcs.SetException(ex))

    let failPendingBatchMessages (ex: exn) =
        if batchMessageContainer.NumMessagesInBatch > 0 then
            batchMessageContainer.Discard(ex)

    let failPendingMessages (ex: exn) =
        while pendingMessages.Count > 0 do
            let msg = pendingMessages.Dequeue()
            failPendingMessage msg ex
        if producerConfig.BatchingEnabled then
            failPendingBatchMessages ex

    let sendTimeoutTimer = new Timer()
    let startSendTimeoutTimer () =
        if sendTimeoutMs > 0.0 then
            sendTimeoutTimer.Interval <- sendTimeoutMs
            sendTimeoutTimer.AutoReset <- true
            sendTimeoutTimer.Elapsed.Add(fun _ -> this.Mb.Post SendTimeoutTick)
            sendTimeoutTimer.Start()

    let batchTimer = new Timer()
    let startSendBatchTimer () =
        batchTimer.Interval <- producerConfig.BatchingMaxPublishDelay.TotalMilliseconds
        batchTimer.AutoReset <- true
        batchTimer.Elapsed.Add(fun _ -> this.Mb.Post SendBatchTick)
        batchTimer.Start()

    let resendMessages () =
        if pendingMessages.Count > 0 then
            Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
            while pendingMessages.Count > 0 do
                let pendingMessage = pendingMessages.Dequeue()
                this.Mb.Post(SendMessage pendingMessage)
        else
            producerCreatedTsc.TrySetResult() |> ignore

    let verifyIfLocalBufferIsCorrupted (msg: PendingMessage<'T>) =
        task {
            use stream = MemoryStreamManager.GetStream()
            use reader = new BinaryReader(stream)
            do! msg.Payload (stream :> Stream) // materialize stream
            let streamSize = stream.Length
            stream.Seek(4L, SeekOrigin.Begin) |> ignore
            let cmdSize = reader.ReadInt32() |> int32FromBigEndian
            stream.Seek((10+cmdSize) |> int64, SeekOrigin.Begin) |> ignore
            let checkSum = reader.ReadInt32() |> int32FromBigEndian
            let checkSumPayload = (int streamSize) - 14 - cmdSize
            let computedCheckSum = CRC32C.Get(0u, stream, checkSumPayload) |> int32
            return checkSum <> computedCheckSum
        }

    let createMessageMetadata (message : MessageBuilder<'a>) (numMessagesInBatch: int option) =
        let sequenceId =
            if message.SequenceId.HasValue then
                %message.SequenceId.Value
            else System.Threading.Interlocked.Exchange(&messageIdGenerator, messageIdGenerator + 1L) |> uint64

        let metadata =
            MessageMetadata (
                SequenceId = sequenceId,
                PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                ProducerName = producerConfig.ProducerName,
                UncompressedSize = (message.Payload.Length |> uint32)
            )
        if protoCompressionType <> pulsar.proto.CompressionType.None then
            metadata.Compression <- protoCompressionType
        if message.Key.IsSome then
            metadata.PartitionKey <- %message.Key.Value.PartitionKey
            metadata.PartitionKeyB64Encoded <- message.Key.Value.IsBase64Encoded
        if message.Properties.Count > 0 then
            for property in message.Properties do
                metadata.Properties.Add(KeyValue(Key = property.Key, Value = property.Value))
        if numMessagesInBatch.IsSome then
            metadata.NumMessagesInBatch <- numMessagesInBatch.Value
        if message.DeliverAt.HasValue then
            metadata.DeliverAtTime <- message.DeliverAt.Value
        match schemaVersion with
        | Some (SchemaVersion sv) -> metadata.SchemaVersion <- sv
        | _ -> ()

        metadata

    let sendOneBatch (batchPayload: byte[], batchCallbacks: BatchCallback<'T>[]) =
        let batchSize = batchCallbacks.Length
        let msgBuilder = MessageBuilder(batchPayload, batchPayload, None)
        let metadata = createMessageMetadata msgBuilder (Some batchSize)
        let sequenceId = %metadata.SequenceId
        let encodedBatchPayload = compressionCodec.Encode msgBuilder.Payload
        if (encodedBatchPayload.Length > maxMessageSize) then
            batchCallbacks
            |> Seq.iter (fun (_, message, tcs) ->
                let ex = InvalidMessageException <| sprintf "Message size is bigger than %i bytes" maxMessageSize
                interceptors.OnSendAcknowledgement(this, message, Unchecked.defaultof<MessageId>, ex)
                tcs.SetException(ex))
        else
            let payload = Commands.newSend producerId sequenceId batchSize metadata encodedBatchPayload
            this.Mb.Post(SendMessage {
                SequenceId = sequenceId
                Payload = payload
                Callback = BatchCallbacks batchCallbacks
                CreatedAt = DateTime.Now
            })

    let canAddToCurrentBatch (msg: MessageBuilder<'T>) =
        batchMessageContainer.HaveEnoughSpace(msg)

    let batchMessageAndSend() =
        let batchSize = batchMessageContainer.NumMessagesInBatch
        Log.Logger.LogDebug("{0} Batching the messages from the batch container with {1} messages", prefix, batchSize)
        if batchSize > 0 then
            if batchMessageContainer.IsMultiBatches then
                batchMessageContainer.CreateOpSendMsgs()
                |> Seq.iter sendOneBatch
            else
                batchMessageContainer.CreateOpSendMsg() |> sendOneBatch
        batchMessageContainer.Clear()

    let updateMaxMessageSize messageSize =
        maxMessageSize <- messageSize
        batchMessageContainer.MaxMessageSize <- messageSize

    let stopProducer() =
        sendTimeoutTimer.Stop()
        batchTimer.Stop()
        connectionHandler.Close()       
        interceptors.Close()
        cleanup(this)
        Log.Logger.LogInformation("{0} stopped", prefix)
        
    let producerOperations = {
        AckReceived = fun (sendReceipt) -> this.Mb.Post(AckReceived sendReceipt)
        TopicTerminatedError = fun () -> this.Mb.Post(TopicTerminatedError)
        RecoverChecksumError = fun (seqId) -> this.Mb.Post(RecoverChecksumError seqId)
        ConnectionClosed = fun (clientCnx) -> this.Mb.Post(ConnectionClosed clientCnx)
    }

    let mb = MailboxProcessor<ProducerMessage<'T>>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with

                | ProducerMessage.ConnectionOpened epoch ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                        updateMaxMessageSize clientCnx.MaxMessageSize
                        clientCnx.AddProducer(producerId, producerOperations)
                        let requestId = Generators.getNextRequestId()
                        try
                            let payload = Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName
                                              producerId requestId schema.SchemaInfo epoch
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            let success = response |> PulsarResponseType.GetProducerSuccess

                            Log.Logger.LogInformation("{0} registered with name {1}", prefix, success.GeneratedProducerName)
                            schemaVersion <- success.SchemaVersion
                            connectionHandler.ResetBackoff()

                            if messageIdGenerator = 0L && producerConfig.InitialSequenceId |> Option.isNone then
                                lastSequenceIdPublished <- success.LastSequenceId
                                messageIdGenerator <- success.LastSequenceId + 1L

                            if producerConfig.BatchingEnabled then
                                startSendBatchTimer()

                            resendMessages()
                        with Flatten ex ->
                            clientCnx.RemoveProducer producerId
                            Log.Logger.LogError(ex, "{0} Failed to create", prefix)
                            match ex with                            
                            | TopicDoesNotExistException reason ->
                                match connectionHandler.ConnectionState with
                                | Failed ->
                                    Log.Logger.LogWarning("{0} Topic doesn't exist exception {1}", prefix, reason)
                                    this.Mb.PostAndAsyncReply(ProducerMessage.Close) |> ignore
                                    producerCreatedTsc.TrySetException(ex) |> ignore
                                | _ -> ()
                            | ProducerBlockedQuotaExceededException reason ->
                                Log.Logger.LogWarning("{0} Topic backlog quota exceeded. {1}", prefix, reason)
                                failPendingMessages(ex)
                            | ProducerBlockedQuotaExceededError reason ->
                                Log.Logger.LogWarning("{0} is blocked on creation because backlog exceeded. {1}", prefix, reason)
                            | _ ->
                                ()

                            match ex with
                            | TopicTerminatedException reason ->
                                Log.Logger.LogWarning("{0} is terminated. {1}", prefix, reason)
                                connectionHandler.Terminate()
                                failPendingMessages(ex)
                                producerCreatedTsc.TrySetException(ex) |> ignore
                                stopProducer()
                            | _ when producerCreatedTsc.Task.IsCompleted || (PulsarClientException.isRetriableError ex && DateTime.Now < createProducerTimeout) ->
                                // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                                // still within the initial timeout budget and we are dealing with a retryable error
                                connectionHandler.ReconnectLater ex
                            | _ ->
                                connectionHandler.Failed()
                                producerCreatedTsc.SetException(ex)
                                stopProducer()
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    return! loop ()

                | ProducerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveProducer(producerId)
                    return! loop ()

                | ProducerMessage.ConnectionFailed ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > createProducerTimeout && producerCreatedTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()
                        stopProducer()
                    else
                        return! loop ()

                | ProducerMessage.StoreBatchItem (message, channel) ->

                    Log.Logger.LogDebug("{0} StoreBatchItem", prefix)
                    let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)
                    let batchItem = { Message = message; Tcs = tcs }
                    if canAddToCurrentBatch(message) then
                        // handle boundary cases where message being added would exceed
                        // batch size and/or max message size
                        if batchMessageContainer.Add(batchItem) then
                            Log.Logger.LogDebug("{0} Max batch container size exceeded", prefix)
                            batchMessageAndSend()
                    else
                       Log.Logger.LogDebug("{0} Closing out batch to accommodate large message with size {1}", prefix, batchItem.Message.Payload.Length)
                       batchMessageAndSend()
                       batchMessageContainer.Add(batchItem) |> ignore
                    channel.Reply(tcs)
                    return! loop ()

                | ProducerMessage.BeginSendMessage (message, channel) ->

                    Log.Logger.LogDebug("{0} BeginSendMessage", prefix)
                    let metadata = createMessageMetadata message None
                    let encodedMessage = compressionCodec.Encode message.Payload
                    let tcs = TaskCompletionSource(TaskContinuationOptions.RunContinuationsAsynchronously)

                    if (encodedMessage.Length > maxMessageSize) then
                        let ex = InvalidMessageException <| sprintf "Message size is bigger than %i bytes" maxMessageSize
                        interceptors.OnSendAcknowledgement(this, message, Unchecked.defaultof<MessageId>, ex)
                        tcs.SetException(ex)
                    else
                        let sequenceId = %metadata.SequenceId
                        let payload = Commands.newSend producerId sequenceId 1 metadata encodedMessage
                        this.Mb.Post(SendMessage { SequenceId = sequenceId; Payload = payload; Callback = SingleCallback (message, tcs); CreatedAt = DateTime.Now })
                    channel.Reply(tcs)
                    return! loop ()

                | ProducerMessage.SendMessage pendingMessage ->

                    Log.Logger.LogDebug("{0} SendMessage id={1}", prefix, %pendingMessage.SequenceId)
                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let! success = clientCnx.Send pendingMessage.Payload
                            if success then
                                Log.Logger.LogDebug("{0} send complete", prefix)
                            else
                                Log.Logger.LogInformation("{0} send failed", prefix)
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    else
                        failPendingMessage pendingMessage (ProducerQueueIsFullError "Producer send queue is full.")
                    return! loop ()

                | ProducerMessage.AckReceived receipt ->

                    let sequenceId = receipt.SequenceId
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = pendingMessage.SequenceId
                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning("{0} Got ack for msg {1}. expecting {2} - queue-size: {3}",
                            prefix, receipt, expectedSequenceId, pendingMessages.Count)
                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Close()
                        | _ -> ()
                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogInformation("{0} Got ack for timed out msg {1} last-seq: {2}",
                            prefix, receipt, expectedSequenceId)
                    else
                        Log.Logger.LogDebug("{0} Received ack for message {1}", prefix, receipt)
                        pendingMessages.Dequeue() |> ignore
                        match pendingMessage.Callback with
                        | SingleCallback (msg, tcs) ->
                            let msgId = { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex; Type = Individual; TopicName = %"" }
                            interceptors.OnSendAcknowledgement(this, msg, msgId, null)
                            tcs.SetResult(msgId)
                        | BatchCallbacks tcss ->
                            tcss
                            |> Array.iter (fun (msgId, msg, tcs) ->
                                let msgId = { LedgerId = receipt.LedgerId; EntryId = receipt.EntryId; Partition = partitionIndex; Type = Cumulative msgId; TopicName = %"" }
                                interceptors.OnSendAcknowledgement(this, msg, msgId, null)
                                tcs.SetResult(msgId))
                    return! loop ()

                | ProducerMessage.RecoverChecksumError sequenceId ->

                    //* Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
                    //* message header-payload again.
                    //* <ul>
                    //* <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
                    //* message</li>
                    //* <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
                    //* So, fail send-message by failing callback</li>
                    //* </ul>
                    Log.Logger.LogWarning("{0} RecoverChecksumError id={1}", prefix, sequenceId)
                    if pendingMessages.Count > 0 then
                        let pendingMessage = pendingMessages.Peek()
                        let expectedSequenceId = pendingMessage.SequenceId
                        if sequenceId = expectedSequenceId then
                            let! corrupted = verifyIfLocalBufferIsCorrupted pendingMessage |> Async.AwaitTask
                            if corrupted then
                                // remove message from pendingMessages queue and fail callback
                                pendingMessages.Dequeue() |> ignore
                                failPendingMessage pendingMessage (ChecksumException "Checksum failed on corrupt message")
                            else
                                Log.Logger.LogDebug("{0} Message is not corrupted, retry send-message with sequenceId {1}", prefix, sequenceId)
                                resendMessages()
                        else
                            Log.Logger.LogDebug("{0} Corrupt message is already timed out {1}", prefix, sequenceId)
                    else
                        Log.Logger.LogDebug("{0} Got send failure for timed out seqId {1}", prefix, sequenceId)
                    return! loop ()

                | ProducerMessage.TopicTerminatedError ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        connectionHandler.Terminate()
                        failPendingMessages(TopicTerminatedException("The topic has been terminated"))
                    return! loop ()

                | ProducerMessage.SendBatchTick ->

                    batchMessageAndSend()
                    return! loop ()

                | ProducerMessage.SendTimeoutTick ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        if pendingMessages.Count > 0 then
                            let firstMessage = pendingMessages.Peek()
                            if firstMessage.CreatedAt.AddMilliseconds(sendTimeoutMs) >= DateTime.Now then
                                // The diff is less than or equal to zero, meaning that the message has been timed out.
                                // Set the callback to timeout on every message, then clear the pending queue.
                                Log.Logger.LogInformation("{0} Message send timed out. Failing {1} messages", prefix, pendingMessages.Count)
                                let ex = TimeoutException "Could not send message to broker within given timeout"
                                failPendingMessages ex
                    return! loop ()

                | ProducerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseProducer producerId requestId
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            clientCnx.RemoveProducer(producerId)
                            connectionHandler.Closed()
                            stopProducer()
                            failPendingMessages(AlreadyClosedException("Producer was already closed"))
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} failed to close", prefix)
                            channel.Reply <| Error ex
                    | _ ->
                        Log.Logger.LogInformation("{0} closing but current state {1}", prefix, connectionHandler.ConnectionState)
                        connectionHandler.Closed()
                        stopProducer()
                        failPendingMessages(AlreadyClosedException("Producer was already closed"))
                        channel.Reply <| Ok()

            }
        loop ()
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do startSendTimeoutTimer()

    member private this.CanAddToBatch (message : MessageBuilder<'T>) =
        producerConfig.BatchingEnabled && not message.DeliverAt.HasValue

    member private this.SendMessage (message : MessageBuilder<'T>) =
        let interceptMsg = interceptors.BeforeSend(this, message)            
        if this.CanAddToBatch(interceptMsg) then
            mb.PostAndAsyncReply(fun channel -> StoreBatchItem (interceptMsg, channel))
        else
            mb.PostAndAsyncReply(fun channel -> BeginSendMessage (interceptMsg, channel))

    member private this.Mb with get(): MailboxProcessor<ProducerMessage<'T>> = mb

    override this.Equals producer =
        producerId = (producer :?> IProducer<'T>).ProducerId

    override this.GetHashCode () = int producerId

    member private this.InitInternal() =
       task {
           do connectionHandler.GrabCnx()
           return! producerCreatedTsc.Task
       }

    static member Init(producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                       partitionIndex: int, lookup: BinaryLookupService, schema: ISchema<'T>,
                       interceptors: ProducerInterceptors<'T>, cleanup: ProducerImpl<'T> -> unit) =
        task {
            let producer = ProducerImpl(producerConfig, clientConfig, connectionPool, partitionIndex, lookup, schema,
                                        interceptors, cleanup)
            do! producer.InitInternal()
            return producer
        }

    interface IProducer<'T> with
        member this.SendAndForgetAsync (message: 'T) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                let! _ = _this.NewMessage message |> this.SendMessage
                return ()
            }

        member this.SendAndForgetAsync (message: MessageBuilder<'T>) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                let! _ = this.SendMessage message
                return ()
            }

        member this.SendAsync (message: 'T) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                let! tcs = _this.NewMessage message |> this.SendMessage
                return! tcs.Task
            }

        member this.SendAsync (message: MessageBuilder<'T>) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                let! tcs = this.SendMessage message
                return! tcs.Task
            }

        member this.NewMessage (value:'T,
            [<Optional; DefaultParameterValue(null:string)>]key:string,
            [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string,string>)>]properties: IReadOnlyDictionary<string, string>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<int64>)>]deliverAt:Nullable<int64>,
            [<Optional; DefaultParameterValue(Nullable():Nullable<uint64>)>]sequenceId:Nullable<uint64>) =            
            keyValueProcessor
            |> Option.map(fun kvp -> kvp.EncodeKeyValue value)
            |> Option.map(fun struct(k, v) -> MessageBuilder(value, v, Some { PartitionKey = %k; IsBase64Encoded = true }, properties, deliverAt))
            |> Option.defaultWith (fun () ->
                MessageBuilder(value, schema.Encode(value),
                                (if String.IsNullOrEmpty(key) then None else Some { PartitionKey = %key; IsBase64Encoded = false }),
                                properties, deliverAt, sequenceId))
                
        member this.ProducerId = producerId

        member this.Topic = %producerConfig.Topic.CompleteTopicName

        member this.LastSequenceId = lastSequenceId

        member this.Name = producerConfig.ProducerName
        
    interface IAsyncDisposable with
        
        member this.DisposeAsync() =     
            match connectionHandler.ConnectionState with
            | Closing | Closed ->
                ValueTask()
            | _ ->
                task {
                    let! result = mb.PostAndAsyncReply(ProducerMessage.Close)
                    match result with
                    | Ok () -> ()
                    | Error ex -> reraize ex 
                } |> ValueTask
            