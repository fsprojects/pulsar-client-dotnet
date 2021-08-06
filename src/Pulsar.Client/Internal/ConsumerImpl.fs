namespace Pulsar.Client.Api

open System.Collections
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Generic
open System
open Pulsar.Client.Schema
open Pulsar.Client.Transaction
open Pulsar.Client.Internal
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open System.IO
open ProtoBuf
open pulsar.proto
open System.Threading
open System.Timers
open ConsumerBase
    
type internal ParseResult<'T> =
    | ParseOk of struct(byte[]*'T)
    | ParseError of CommandAck.ValidationError
    
type internal ConsumerTickType =
    | StatTick
    | ChunkTick
    
type internal ConsumerMessage<'T> =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | ReachedEndOfTheTopic
    | MessageReceived of RawMessage * ClientCnx
    | Receive of CancellationToken * AsyncReplyChannel<ResultOrException<Message<'T>>>
    | BatchReceive of CancellationToken * AsyncReplyChannel<ResultOrException<Messages<'T>>>
    | SendBatchByTimeout
    | Acknowledge of MessageId * AckType * Transaction option * AsyncReplyChannel<Task<Unit>> option
    | NegativeAcknowledge of MessageId
    | RedeliverUnacknowledged of RedeliverSet * AsyncReplyChannel<unit>
    | RedeliverAllUnacknowledged of AsyncReplyChannel<unit>
    | SeekAsync of SeekType * AsyncReplyChannel<ResultOrException<unit>>
    | HasMessageAvailable of AsyncReplyChannel<Task<bool>>
    | ActiveConsumerChanged of bool
    | Close of AsyncReplyChannel<ResultOrException<unit>>
    | Unsubscribe of AsyncReplyChannel<ResultOrException<unit>>
    | Tick of ConsumerTickType
    | GetStats of AsyncReplyChannel<ConsumerStats>
    | ReconsumeLater of Message<'T> * AckType * TimeStamp * AsyncReplyChannel<Task<unit>>
    | RemoveWaiter of Waiter<'T>
    | RemoveBatchWaiter of BatchWaiter<'T>
    | AckReceipt of RequestId
    | AckError of RequestId * exn
    | ClearIncomingMessagesAndGetMessageNumber of AsyncReplyChannel<int>
    | IncreaseAvailablePermits of int

type internal ConsumerInitInfo<'T> =
    {
        TopicName: TopicName
        Schema: ISchema<'T>
        SchemaProvider: MultiVersionSchemaInfoProvider option
        Metadata: PartitionedTopicMetadata
    }

type internal ConsumerImpl<'T> (consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration,
                           topicName: TopicName, connectionPool: ConnectionPool,
                           partitionIndex: int, hasParentConsumer: bool, startMessageId: MessageId option, 
                           startMessageRollbackDuration: TimeSpan, lookup: BinaryLookupService,
                           createTopicIfDoesNotExist: bool, schema: ISchema<'T>,
                           schemaProvider: MultiVersionSchemaInfoProvider option,
                           interceptors: ConsumerInterceptors<'T>, cleanup: ConsumerImpl<'T> -> unit) as this =

    [<Literal>]
    let MAX_REDELIVER_UNACKNOWLEDGED = 1000
    let _this = this :> IConsumer<'T>
    let consumerName = getConsumerName consumerConfig.ConsumerName
    let consumerId = Generators.getNextConsumerId()
    let incomingMessages = Queue<Message<'T>>()
    let waiters = LinkedList<Waiter<'T>>()
    let batchWaiters = LinkedList<BatchWaiter<'T>>()
    let subscribeTsc = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let ackRequests = Dictionary<RequestId, MessageId * TxnId * TaskCompletionSource<unit>>()
    let prefix = $"consumer({consumerId}, {consumerName}, {partitionIndex})"
    let subscribeTimeout = DateTime.Now.Add(clientConfig.OperationTimeout)
    let mutable hasReachedEndOfTopic = false
    let mutable avalablePermits = 0
    let mutable startMessageId = startMessageId
    let mutable lastMessageIdInBroker = MessageId.Earliest
    let mutable lastDequeuedMessageId = MessageId.Earliest
    let mutable duringSeek = None
    let initialStartMessageId = startMessageId
    let mutable incomingMessagesSize = 0L
    let receiverQueueRefillThreshold = consumerConfig.ReceiverQueueSize / 2
    let deadLettersProcessor = consumerConfig.DeadLetterProcessor topicName
    let isDurable = consumerConfig.SubscriptionMode = SubscriptionMode.Durable
    let stats =
        if clientConfig.StatsInterval = TimeSpan.Zero then
            ConsumerStatsImpl.CONSUMER_STATS_DISABLED
        else
            ConsumerStatsImpl(prefix) :> IConsumerStatsRecorder
    
    let statTimer = new Timer()
    let chunkTimer = new Timer()
    let startStatTimer () =
        if clientConfig.StatsInterval <> TimeSpan.Zero then
            statTimer.Interval <- clientConfig.StatsInterval.TotalMilliseconds
            statTimer.AutoReset <- true
            statTimer.Elapsed.Add(fun _ -> this.Mb.Post(Tick StatTick))
            statTimer.Start()
    
    let startChunkTimer () =
        if consumerConfig.ExpireTimeOfIncompleteChunkedMessage <> TimeSpan.Zero then
            chunkTimer.Interval <- consumerConfig.ExpireTimeOfIncompleteChunkedMessage.TotalMilliseconds
            chunkTimer.AutoReset <- true
            chunkTimer.Elapsed.Add(fun _ -> this.Mb.Post(Tick ChunkTick))
            chunkTimer.Start()
    
    let keyValueProcessor = KeyValueProcessor.GetInstance schema

    let wrapPostAndReply (mbAsyncReply: Async<ResultOrException<'A>>) =
        async {
            match! mbAsyncReply with
            | Ok msg ->
                return msg
            | Error ex ->
                return reraize ex
        }
    
    let connectionHandler =
        ConnectionHandler(prefix,
                          connectionPool,
                          lookup,
                          topicName.CompleteTopicName,
                          (fun _ -> this.Mb.Post(ConsumerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ConsumerMessage.ConnectionFailed ex)),
                          Backoff({ BackoffConfig.Default with
                                        Initial = clientConfig.InitialBackoffInterval
                                        Max = clientConfig.MaxBackoffInterval }))

    let consumerTxnOperations =
        {
            ClearIncomingMessagesAndGetMessageNumber = fun () ->
                ConsumerMessage.ClearIncomingMessagesAndGetMessageNumber |> this.Mb.PostAndAsyncReply
            IncreaseAvailablePermits = fun permits ->
                ConsumerMessage.IncreaseAvailablePermits permits |> this.Mb.Post
        }
    
    let hasMoreMessages (lastMessageIdInBroker: MessageId) (lastDequeuedMessage: MessageId) (inclusive: bool) =
        if (inclusive && lastMessageIdInBroker >= lastDequeuedMessage && lastMessageIdInBroker.EntryId <> %(-1L)) then
            true
        elif (not inclusive && lastMessageIdInBroker > lastDequeuedMessage && lastMessageIdInBroker.EntryId <> %(-1L)) then
            true
        else
            false

    let increaseAvailablePermits delta =
        avalablePermits <- avalablePermits + delta
        if avalablePermits >= receiverQueueRefillThreshold then
            this.SendFlowPermits avalablePermits
            avalablePermits <- 0

    /// Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that was not seen by the application
    let clearReceiverQueue() =
        let nextMsg =
            if incomingMessages.Count > 0 then
                let nextMessageInQueue = incomingMessages.Dequeue().MessageId
                incomingMessagesSize <- 0L
                incomingMessages.Clear()
                Some nextMessageInQueue
            else
                None
        match duringSeek with
        | Some _ as seekMsgId ->
            duringSeek <- None
            seekMsgId
        | None when isDurable ->
            startMessageId
        | _  ->        
            match nextMsg with
            | Some nextMessageInQueue ->                
                let previousMessage =
                    match nextMessageInQueue.Type with
                    | Batch (index, acker) ->
                        // Get on the previous message within the current batch
                        { nextMessageInQueue with Type = Batch(index - %1, acker) }
                    | MessageIdType.Single ->
                        // Get on previous message in previous entry
                        { nextMessageInQueue with EntryId = nextMessageInQueue.EntryId - %1L }
                Some previousMessage
            | None ->
                if lastDequeuedMessageId <> MessageId.Earliest then
                    // If the queue was empty we need to restart from the message just after the last one that has been dequeued
                    // in the past
                    Some lastDequeuedMessageId
                else
                    // No message was received or dequeued by this consumer. Next message would still be the startMessageId
                    startMessageId
    
    let getLastMessageIdAsync() =
        
        let rec internalGetLastMessageIdAsync(backoff: Backoff, remainingTimeMs: int) =
            async {
                match connectionHandler.ConnectionState with
                | Ready clientCnx ->
                    let requestId = Generators.getNextRequestId()
                    let payload = Commands.newGetLastMessageId consumerId requestId
                    try
                        let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                        return response |> PulsarResponseType.GetLastMessageId
                    with
                    | Flatten ex ->
                        Log.Logger.LogError(ex, "{0} failed getLastMessageId", prefix)
                        return reraize ex
                | _ ->
                    let nextDelay = Math.Min(backoff.Next(), remainingTimeMs)
                    if nextDelay <= 0 then
                        return
                            "Couldn't get the last message id withing configured timeout"
                            |> TimeoutException
                            |> raise
                    else
                        Log.Logger.LogWarning("{0} Could not get connection while GetLastMessageId -- Will try again in {1} ms",
                                              prefix, nextDelay)    
                        do! Async.Sleep nextDelay
                        return! internalGetLastMessageIdAsync(backoff, remainingTimeMs - nextDelay)
            }
        
        match connectionHandler.ConnectionState with
        | Closing | Closed ->
            "Consumer is already closed"
            |> AlreadyClosedException
            |> Task.FromException<LastMessageIdResult>
        | _ ->
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        Max = (clientConfig.OperationTimeout + clientConfig.OperationTimeout)}
            internalGetLastMessageIdAsync(backoff, int clientConfig.OperationTimeout.TotalMilliseconds) |> Async.StartAsTask
    
    let redeliverMessages messages =
        this.Mb.PostAndAsyncReply(fun channel -> RedeliverUnacknowledged (messages, channel))
        |> Async.StartImmediate

    let unAckedMessageRedeliver messages =
        interceptors.OnAckTimeoutSend(this, messages)
        redeliverMessages messages

    let negativeAcksRedeliver messages =
        interceptors.OnNegativeAcksSend(this, messages)
        redeliverMessages messages
    
    let unAckedMessageTracker =
        if consumerConfig.AckTimeout > TimeSpan.Zero then
            if consumerConfig.AckTimeoutTickTime > TimeSpan.Zero then
                let tickDuration = if consumerConfig.AckTimeout > consumerConfig.AckTimeoutTickTime then consumerConfig.AckTimeoutTickTime else consumerConfig.AckTimeout
                UnAckedMessageTracker(prefix, consumerConfig.AckTimeout, tickDuration, unAckedMessageRedeliver) :> IUnAckedMessageTracker
            else
                UnAckedMessageTracker(prefix, consumerConfig.AckTimeout, consumerConfig.AckTimeout, unAckedMessageRedeliver) :> IUnAckedMessageTracker
        else
            UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED

    let negativeAcksTracker = NegativeAcksTracker(prefix, consumerConfig.NegativeAckRedeliveryDelay, negativeAcksRedeliver)
    
    let getConnectionState() = connectionHandler.ConnectionState
    let sendAckPayload (cnx: ClientCnx) payload = cnx.Send payload

    let acksGroupingTracker =
        if topicName.IsPersistent then
            AcknowledgmentsGroupingTracker(prefix, consumerId, consumerConfig.AcknowledgementsGroupTime, getConnectionState, sendAckPayload) :> IAcknowledgmentsGroupingTracker
        else
            AcknowledgmentsGroupingTracker.NonPersistentAcknowledgmentGroupingTracker

    let trackMessage msgId =
        if not hasParentConsumer then
            unAckedMessageTracker.Add msgId |> ignore
            
    let untrackMessage msgId =
        if not hasParentConsumer then
            unAckedMessageTracker.Remove msgId |> ignore
    
    let untrackMessagesTill msgId =
        if not hasParentConsumer then
            unAckedMessageTracker.RemoveMessagesTill msgId
        else
            0
    
    let sendAcknowledge (messageId: MessageId) ackType properties =
        match ackType with
        | Individual ->
            match messageId.Type with
            | MessageIdType.Single ->
                untrackMessage messageId
                stats.IncrementNumAcksSent(1)
            | Batch (_, batch) ->
                let batchSize = batch.GetBatchSize()
                for i in 0..batchSize-1 do
                    untrackMessage { messageId with Type = Batch(%i, batch) }
                stats.IncrementNumAcksSent(batchSize)
            interceptors.OnAcknowledge(this, messageId, null)
            deadLettersProcessor.RemoveMessage messageId
        | AckType.Cumulative ->
            interceptors.OnAcknowledgeCumulative(this, messageId, null)
            let count = untrackMessagesTill messageId
            stats.IncrementNumAcksSent(count)
        acksGroupingTracker.AddAcknowledgment(messageId, ackType, properties)
        // Consumer acknowledgment operation immediately succeeds. In any case, if we're not able to send ack to broker,
        // the messages will be re-delivered
        
    let ackOrTrack msgId autoAck =
        if autoAck then
            Log.Logger.LogInformation("{0} Removing chunk message-id {1}", prefix, msgId)
            sendAcknowledge msgId Individual EmptyProperties
        else
            trackMessage msgId
            
    let chunkedMessageTracker = ChunkedMessageTracker(prefix, consumerConfig.MaxPendingChunkedMessage,
                                                      consumerConfig.AutoAckOldestChunkedMessageOnQueueFull,
                                                      consumerConfig.ExpireTimeOfIncompleteChunkedMessage,
                                                      ackOrTrack)
    
    let markAckForBatchMessage (msgId: MessageId) ((batchIndex, batchAcker): BatchDetails) ackType properties =
        let isAllMsgsAcked =
            match ackType with
            | Individual ->
                batchAcker.AckIndividual(batchIndex)
            | AckType.Cumulative ->
                batchAcker.AckCumulative(batchIndex)
        let outstandingAcks = batchAcker.GetOutstandingAcks()
        let batchSize = batchAcker.GetBatchSize()
        if isAllMsgsAcked then
            Log.Logger.LogDebug("{0} can ack message acktype {1}, cardinality {2}, length {3}",
                prefix, ackType, outstandingAcks, batchSize)
            true
        else
            match ackType with
            | AckType.Cumulative ->
                if not batchAcker.PrevBatchCumulativelyAcked then
                    sendAcknowledge msgId.PrevBatchMessageId ackType properties
                    Log.Logger.LogDebug("{0} update PrevBatchCumulativelyAcked", prefix)
                    batchAcker.PrevBatchCumulativelyAcked <- true
                interceptors.OnAcknowledgeCumulative(this, msgId, null)
            | Individual ->
                interceptors.OnAcknowledge(this, msgId, null)
            Log.Logger.LogDebug("{0} cannot ack message acktype {1}, cardinality {2}, length {3}",
                prefix, ackType, outstandingAcks, batchSize)
            false

    let doTransactionAcknowledgeForResponse ackType properties txnId (messageId: MessageId) =
        match connectionHandler.ConnectionState with
        | Ready clientCnx ->
            let requestId = Generators.getNextRequestId()
            let command =
                match messageId.Type with
                | Batch (batchIndex, batchAcker) ->
                    let ackSet =
                        match ackType with
                        | Cumulative ->
                            batchAcker.AckCumulative(batchIndex) |> ignore
                            let bitSet = BitArray(batchAcker.GetBatchSize(), true)
                            for i in 0 .. %batchIndex do
                                bitSet.[i] <- false
                            bitSet
                        | Individual ->
                            let bitSet = BitArray(batchAcker.GetBatchSize(), true)
                            bitSet.[%batchIndex] <- false
                            bitSet
                    let allBitsAreZero =
                        let mutable result = true
                        let mutable i = 0
                        while result && i < ackSet.Length do
                            result <- not ackSet.[i]
                            i <- i + 1
                        result
                    let adjustedSet =
                        if allBitsAreZero then
                            // hack to conform Java
                            [||]
                        else
                            ackSet |> toLongArray
                    Commands.newAck consumerId messageId.LedgerId messageId.EntryId ackType properties adjustedSet
                            None (Some txnId) (Some requestId) (batchAcker.GetBatchSize() |> Some)
                | _ ->
                    Commands.newAck consumerId messageId.LedgerId messageId.EntryId ackType properties null
                            None (Some txnId) (Some requestId) None
            let tcs = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            ackRequests.Add(requestId, (messageId, txnId, tcs))
            match ackType with
            | Individual -> unAckedMessageTracker.Remove(messageId) |> ignore
            | AckType.Cumulative -> unAckedMessageTracker.RemoveMessagesTill(messageId) |> ignore
            clientCnx.SendAndForget(command)
            tcs.Task
        | _ ->
            Log.Logger.LogWarning("{0} not ready, can't do transactionAcknowledge. State: {1}",
                                  prefix, connectionHandler.ConnectionState)
            Task.FromException<unit>(NotConnectedException("TransactionAcknowledge failed"))
    
    let trySendAcknowledge ackType properties (txnOption: Transaction option) (messageId: MessageId) =
        match txnOption with
        | Some txn ->
            doTransactionAcknowledgeForResponse ackType properties txn.Id messageId
        | None ->
            match messageId.Type with
            | Batch batchDetails when not (markAckForBatchMessage messageId batchDetails ackType properties) ->
                if consumerConfig.BatchIndexAcknowledgmentEnabled then
                    acksGroupingTracker.AddBatchIndexAcknowledgment(messageId, ackType, properties)
                // other messages in batch are still pending ack.
            | _ ->
                sendAcknowledge messageId ackType properties
                Log.Logger.LogDebug("{0} acknowledged message - {1}, acktype {2}", prefix, messageId, ackType)
            Task.FromResult()

    let isPriorEntryIndex idx =
        match startMessageId with
        | None -> false
        | Some startMsgId ->
            if consumerConfig.ResetIncludeHead then
                idx < startMsgId.EntryId
            else
                idx <= startMsgId.EntryId

    let isPriorBatchIndex idx =
        match startMessageId with
        | None -> false
        | Some startMsgId ->
            match startMsgId.Type with
            | MessageIdType.Single -> false
            | Batch (batchIndex, _) ->
                if consumerConfig.ResetIncludeHead then
                    idx < batchIndex
                else
                    idx <= batchIndex

    let isSameEntry (msgId: MessageId) =
        match startMessageId with
        | None ->
            false
        | Some startMsgId ->
                startMsgId.LedgerId = msgId.LedgerId
                && startMsgId.EntryId = msgId.EntryId
    
    let getSchemaVersionBytes (schemaVersion: SchemaVersion option) =
        schemaVersion
        |> Option.map (fun sv -> sv.Bytes)
        |> Option.defaultValue null
    
    let clearDeadLetters() = deadLettersProcessor.ClearMessages()
    
    let clearAckRequests() =
        for KeyValue(_, (_, _, tcs)) in ackRequests do
            stats.IncrementNumAcksFailed()
            tcs.SetException(MessageAcknowledgeException "Consumer was closed")
        ackRequests.Clear()

    let getNewIndividualMsgIdWithPartition messageId =
        { messageId with Type = MessageIdType.Single; Partition = partitionIndex; TopicName = %"" }

    let processPossibleToDLQ (messageId : MessageId) =
        let acknowledge = trySendAcknowledge Individual EmptyProperties None
        task {
            try
                return! deadLettersProcessor.ProcessMessage(messageId, acknowledge)
            with Flatten ex ->
                Log.Logger.LogError(ex, "Failed to send DLQ message to {0} for message id {1}",
                                    deadLettersProcessor.TopicName, messageId)
                return Task.FromResult(false)
        }
        
    let getRedeliveryMessageIdData ids =
        
        task {
            let isDeadTasks = ResizeArray()
            for messageId in ids do
                let! isDeadTask = processPossibleToDLQ messageId
                isDeadTasks.Add (messageId, isDeadTask)
            let! results =
                isDeadTasks
                |> Seq.map(fun (messageId, isDeadTask) ->
                        task {
                            let! isDead = isDeadTask
                            return
                                if isDead then
                                    None
                                else
                                    MessageIdData(
                                        Partition = messageId.Partition,
                                        ledgerId = uint64 %messageId.LedgerId,
                                        entryId = uint64 %messageId.EntryId)
                                    |> Some
                        }
                    )
                |> Task.WhenAll
            return
                results
                |> Array.choose id
        }

    let enqueueMessage (msg: Message<'T>) =
        incomingMessagesSize <- incomingMessagesSize + msg.Data.LongLength
        incomingMessages.Enqueue(msg)

    let dequeueMessage() =
        let msg = incomingMessages.Dequeue()
        incomingMessagesSize <- incomingMessagesSize - msg.Data.LongLength
        msg

    let hasEnoughMessagesForBatchReceive() =
        hasEnoughMessagesForBatchReceive consumerConfig.BatchReceivePolicy incomingMessages.Count incomingMessagesSize
        
    /// Record the event that one message has been processed by the application.
    /// Periodically, it sends a Flow command to notify the broker that it can push more messages
    let messageProcessed (msg: Message<'T>) =
        lastDequeuedMessageId <- msg.MessageId
        if consumerConfig.ReceiverQueueSize > 0 then
            //don't increase for zero queue consumer
            increaseAvailablePermits 1
        stats.UpdateNumMsgsReceived(msg.Data.Length)
        trackMessage msg.MessageId

    let replyWithBatch (ch: AsyncReplyChannel<ResultOrException<Messages<'T>>>) =
        let messages = Messages(consumerConfig.BatchReceivePolicy.MaxNumMessages, consumerConfig.BatchReceivePolicy.MaxNumBytes)
        let mutable shouldContinue = true
        while shouldContinue && incomingMessages.Count > 0 do
            let msgPeeked = incomingMessages.Peek()
            if (messages.CanAdd(msgPeeked)) then
                let msg = dequeueMessage()
                messageProcessed msg
                messages.Add(interceptors.BeforeConsume(this, msg))
            else
                shouldContinue <- false
        Log.Logger.LogDebug("{0} BatchFormed with size {1}", prefix, messages.Size)
        ch.Reply(Ok messages)
    
    let removeExpiredMessagesFromQueue (msgIds: RedeliverSet) =
        if incomingMessages.Count > 0 then
            let peek = incomingMessages.Peek()
            if msgIds.Contains peek.MessageId then
                // try not to remove elements that are added while we remove
                let mutable finish = false
                let mutable messagesFromQueue = 0
                while not finish && incomingMessages.Count > 0 do
                    let message = dequeueMessage()
                    messagesFromQueue <- messagesFromQueue + 1
                    if msgIds.Contains(message.MessageId) |> not then
                        msgIds.Add(message.MessageId) |> ignore
                        finish <- true
                messagesFromQueue
            else
                // first message is not expired, then no message is expired in queue.
                0
        else
            0

    let replyWithMessage (channel: AsyncReplyChannel<ResultOrException<Message<'T>>>) message =
        messageProcessed message
        let interceptMsg = interceptors.BeforeConsume(this, message)
        channel.Reply (Ok interceptMsg)

    let stopConsumer () =
        unAckedMessageTracker.Close()
        acksGroupingTracker.Close()
        clearDeadLetters()
        clearAckRequests()
        negativeAcksTracker.Close()
        connectionHandler.Close()
        interceptors.Close()
        statTimer.Stop()
        chunkTimer.Stop()
        cleanup(this)
        while waiters.Count > 0 do
            let waitingChannel = waiters |> dequeueWaiter
            waitingChannel.Reply(Error (AlreadyClosedException("Consumer is already closed") :> exn))
        while batchWaiters.Count > 0 do
            let batchWaitingChannel = batchWaiters |> dequeueBatchWaiter
            batchWaitingChannel.Reply(Error (AlreadyClosedException("Consumer is already closed") :> exn))
        Log.Logger.LogInformation("{0} stopped", prefix)

    let decryptMessage (rawMessage:RawMessage) =
        if rawMessage.Metadata.EncryptionKeys.Length = 0 then
            Ok rawMessage
        else
            try
                match consumerConfig.MessageDecryptor with
                | Some msgCrypto -> 
                    let encryptionKeys = rawMessage.Metadata.EncryptionKeys
                    let encMsg = EncryptedMessage(rawMessage.Payload, encryptionKeys,
                                                  rawMessage.Metadata.EncryptionAlgo, rawMessage.Metadata.EncryptionParam)
                    let decryptPayload = msgCrypto.Decrypt(encMsg)
                    { rawMessage with Payload = decryptPayload } |> Ok
                | None -> 
                    raise <| CryptoException "Message is encrypted, but no encryption configured"
            with ex ->
                Log.Logger.LogWarning(ex, "{0} Decryption exception {1}", prefix, rawMessage.MessageId)
                Error ex
    
    let decompressMessage (rawMessage: RawMessage) isChunked =
        if isChunked then
            Ok rawMessage
        else
            try
                let compressionCodec = rawMessage.Metadata.CompressionType |> CompressionCodec.get
                let uncompressedPayload = compressionCodec.Decode(rawMessage.Metadata.UncompressedMessageSize, rawMessage.Payload)
                Ok { rawMessage with Payload = uncompressedPayload }
            with ex ->
                Log.Logger.LogError(ex, "{0} Decompression exception {1}", prefix, rawMessage.MessageId)
                Error ex
        
    let discardCorruptedMessage (msgId: MessageId) (clientCnx: ClientCnx) err =
        async {
            let command = Commands.newAck consumerId msgId.LedgerId msgId.EntryId Individual
                            EmptyProperties null (Some err) None None None
            let! discardResult = clientCnx.Send command
            if discardResult then
                Log.Logger.LogInformation("{0} Message {1} was discarded due to {2}", prefix, msgId, err)
            else
                Log.Logger.LogWarning("{0} Unable to discard {1} due to {2}", prefix, msgId, err)
            stats.IncrementNumReceiveFailed()
        }
        
    let getSchemaDecodeFunction (metadata: Metadata) =
        async {
            // mutable workaround for unsupported let! inside let
            let mutable schemaDecodeFunction = Unchecked.defaultof<(byte[]->'T)>
            if schemaProvider.IsNone || metadata.SchemaVersion.IsNone then
                schemaDecodeFunction <- schema.Decode
            else
                let schemaVersion = metadata.SchemaVersion.Value
                try
                    let! specificSchemaOption = schemaProvider.Value.GetSchemaByVersion(schema, schemaVersion) |> Async.AwaitTask
                    schemaDecodeFunction <-
                        match specificSchemaOption with
                        | Some specificSchema -> specificSchema.Decode
                        | None -> schema.Decode
                with Flatten ex ->
                    Log.Logger.LogError(ex, "{0} Couldn't get schema by version", prefix)
                    schemaDecodeFunction <- fun _ -> raise <| SchemaSerializationException ex.Message
            return schemaDecodeFunction
        }
           
    let processMessageChunk (rawMessage: RawMessage) msgId =
        match chunkedMessageTracker.GetContext(rawMessage.Metadata) with
        | Ok chunkedContext ->
            let compressionCodec = rawMessage.Metadata.CompressionType |> CompressionCodec.get
            match chunkedMessageTracker.MessageReceived(rawMessage, msgId, chunkedContext, compressionCodec) with
            | Some payloadAndMessageId ->
                Some payloadAndMessageId
            | None ->
                increaseAvailablePermits 1
                None
        | Error error ->
            // discard message if chunk is out-of-order
            Log.Logger.LogWarning("{0} {1} msgId = {2}", prefix, error, rawMessage.MessageId)
            increaseAvailablePermits 1
            let publishDateTime = convertToDateTime %rawMessage.Metadata.PublishTime
            if consumerConfig.ExpireTimeOfIncompleteChunkedMessage > TimeSpan.Zero &&
                DateTime.UtcNow > publishDateTime.Add(consumerConfig.ExpireTimeOfIncompleteChunkedMessage) then
                sendAcknowledge rawMessage.MessageId Individual EmptyProperties
            else
                trackMessage rawMessage.MessageId
            None
            
    let handleSingleMessagePayload (rawMessage: RawMessage) msgId payload hasWaitingChannel hasWaitingBatchChannel schemaDecodeFunction =
        if duringSeek.IsSome || (isSameEntry(rawMessage.MessageId) && isPriorEntryIndex(rawMessage.MessageId.EntryId)) then
            // We need to discard entries that were prior to startMessageId
            Log.Logger.LogInformation("{0} Ignoring message from before the startMessageId: {1}", prefix, startMessageId)
        else
            let msgKey = rawMessage.MessageKey
            let getValue () =
                keyValueProcessor
                |> Option.map (fun kvp -> kvp.DecodeKeyValue(msgKey, payload) :?> 'T)
                |> Option.defaultWith (fun () -> schemaDecodeFunction payload)
            let message = Message(
                            msgId,
                            payload,
                            %msgKey,
                            rawMessage.IsKeyBase64Encoded,
                            rawMessage.Properties,
                            EncryptionContext.FromMetadata rawMessage.Metadata,
                            getSchemaVersionBytes rawMessage.Metadata.SchemaVersion,
                            rawMessage.Metadata.SequenceId,
                            rawMessage.Metadata.OrderingKey,
                            rawMessage.Metadata.PublishTime,
                            rawMessage.Metadata.EventTime,
                            getValue
                        )
            if (rawMessage.RedeliveryCount >= deadLettersProcessor.MaxRedeliveryCount) then
                deadLettersProcessor.AddMessage(message.MessageId, message)
            if hasWaitingChannel then
                let waitingChannel = waiters |> dequeueWaiter
                if (incomingMessages.Count = 0) then
                    replyWithMessage waitingChannel message
                else
                    enqueueMessage message
                    replyWithMessage waitingChannel <| dequeueMessage()
            else
                enqueueMessage message
                if hasWaitingBatchChannel && hasEnoughMessagesForBatchReceive() then
                    let ch = batchWaiters |> dequeueBatchWaiter
                    replyWithBatch ch
            
    let handleMessagePayload (rawMessage: RawMessage) msgId hasWaitingChannel hasWaitingBatchChannel
                                isMessageUndecryptable isChunkedMessage schemaDecodeFunction =
        if acksGroupingTracker.IsDuplicate msgId then
            Log.Logger.LogWarning("{0} Ignoring message as it was already being acked earlier by same consumer {1}", prefix, msgId)
            increaseAvailablePermits rawMessage.Metadata.NumMessages
        else
            if isMessageUndecryptable || (rawMessage.Metadata.NumMessages = 1 && not rawMessage.Metadata.HasNumMessagesInBatch) then
                // right now, chunked messages are only supported by non-shared subscription
                if isChunkedMessage then
                    match processMessageChunk rawMessage msgId with
                    | Some (chunkedPayload, msgIdWithChunk) ->
                        handleSingleMessagePayload rawMessage msgIdWithChunk chunkedPayload hasWaitingChannel hasWaitingBatchChannel schemaDecodeFunction
                    | None ->
                        ()
                else
                    handleSingleMessagePayload rawMessage msgId rawMessage.Payload hasWaitingChannel hasWaitingBatchChannel schemaDecodeFunction
            elif rawMessage.Metadata.NumMessages > 0 then
                // handle batch message enqueuing; uncompressed payload has all messages in batch
                try
                    this.ReceiveIndividualMessagesFromBatch rawMessage schemaDecodeFunction
                with ex ->
                    Log.Logger.LogError(ex, "{0} Batch reading exception {1}", prefix, msgId)
                    raise <| BatchDeserializationException "Batch reading exception"
                // try respond to channel
                if hasWaitingChannel && incomingMessages.Count > 0 then
                    let waitingChannel = waiters |> dequeueWaiter
                    replyWithMessage waitingChannel <| dequeueMessage()
                elif hasWaitingBatchChannel && hasEnoughMessagesForBatchReceive() then
                    let ch = batchWaiters |> dequeueBatchWaiter
                    replyWithBatch ch
            else
                Log.Logger.LogWarning("{0} Received message with nonpositive numMessages: {1}", prefix, rawMessage.Metadata.NumMessages)
    
    let tryHandleMessagePayload rawMessage msgId hasWaitingChannel hasWaitingBatchChannel
                                isMessageUndecryptable isChunkedMessage schemaDecodeFunction clientCnx =
        try
            handleMessagePayload rawMessage msgId hasWaitingChannel hasWaitingBatchChannel
                isMessageUndecryptable isChunkedMessage schemaDecodeFunction
            async.Return ()
        with
        | :? BatchDeserializationException ->
            discardCorruptedMessage msgId clientCnx CommandAck.ValidationError.BatchDeSerializeError
        
        
    let consumerOperations = {
        MessageReceived = fun (rawMessage) -> this.Mb.Post(MessageReceived rawMessage)
        ReachedEndOfTheTopic = fun () -> this.Mb.Post(ReachedEndOfTheTopic)
        ActiveConsumerChanged = fun (isActive) -> this.Mb.Post(ActiveConsumerChanged isActive)
        ConnectionClosed = fun (clientCnx) -> this.Mb.Post(ConnectionClosed clientCnx)
        AckError = fun (reqId, ex) -> this.Mb.Post(AckError(reqId, ex))
        AckReceipt = fun (reqId) -> this.Mb.Post(AckReceipt(reqId))
    }
    
    let mb = MailboxProcessor<ConsumerMessage<'T>>.Start(fun inbox ->

        let rec loop () =
            async {
                match! inbox.Receive() with
                
                | ConsumerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting subscribe to topic {1}", prefix, topicName)
                        clientCnx.AddConsumer(consumerId, consumerOperations)
                        let requestId = Generators.getNextRequestId()
                        startMessageId <- clearReceiverQueue()
                        clearDeadLetters()
                        let msgIdData =
                            if isDurable then
                                null
                            else
                                match startMessageId with
                                | None ->
                                    Log.Logger.LogWarning("{0} Start messageId is missing", prefix)
                                    null
                                | Some msgId ->
                                    let data = MessageIdData(ledgerId = uint64 %msgId.LedgerId, entryId = uint64 %msgId.EntryId)
                                    match msgId.Type with
                                    | MessageIdType.Single ->
                                        ()
                                    | Batch (index, _) ->
                                        data.BatchIndex <- %index
                                    data
                        // startMessageRollbackDurationInSec should be consider only once when consumer connects to first time
                        let startMessageRollbackDuration =
                            if startMessageRollbackDuration > TimeSpan.Zero && startMessageId = initialStartMessageId then
                                startMessageRollbackDuration
                            else
                                TimeSpan.Zero
                        let payload =
                            Commands.newSubscribe
                                topicName.CompleteTopicName consumerConfig.SubscriptionName
                                consumerId requestId consumerName consumerConfig.SubscriptionType
                                consumerConfig.SubscriptionInitialPosition consumerConfig.ReadCompacted msgIdData isDurable
                                startMessageRollbackDuration createTopicIfDoesNotExist consumerConfig.KeySharedPolicy
                                schema.SchemaInfo consumerConfig.PriorityLevel
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            this.ConsumerIsReconnectedToBroker()
                            connectionHandler.ResetBackoff()
                            let initialFlowCount = consumerConfig.ReceiverQueueSize
                            subscribeTsc.TrySetResult() |> ignore
                            if initialFlowCount <> 0 then
                                this.SendFlowPermits initialFlowCount
                        with Flatten ex ->
                            clientCnx.RemoveConsumer consumerId
                            Log.Logger.LogError(ex, "{0} failed to subscribe to topic", prefix)
                            match ex with
                            | _ when (PulsarClientException.isRetriableError ex && DateTime.Now < subscribeTimeout) ->
                                connectionHandler.ReconnectLater ex
                            | _ when not subscribeTsc.Task.IsCompleted ->
                                // unable to create new consumer, fail operation
                                connectionHandler.Failed()
                                subscribeTsc.SetException(ex)
                                stopConsumer()
                            | :? TopicDoesNotExistException ->
                                // The topic was deleted after the consumer was created, and we're
                                // not allowed to recreate the topic. This can happen in few cases:
                                //  * Regex consumer getting error after topic gets deleted
                                //  * Regular consumer after topic is manually delete and with
                                //    auto-topic-creation set to false
                                // No more retries are needed in this case.
                                connectionHandler.Failed()
                                stopConsumer()
                                Log.Logger.LogWarning("{0} Closed consumer because topic does not exist anymore. {1}", prefix, ex.Message)
                            | _ ->
                                // consumer was subscribed and connected but we got some error, keep trying
                                connectionHandler.ReconnectLater ex
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    
                    if connectionHandler.ConnectionState <> Failed then
                        return! loop ()
                    
                | ConsumerMessage.MessageReceived (rawMessage, clientCnx) ->
                    
                    let hasWaitingChannel = waiters.Count > 0
                    let hasWaitingBatchChannel = batchWaiters.Count > 0
                    let msgId = getNewIndividualMsgIdWithPartition rawMessage.MessageId
                    Log.Logger.LogDebug("{0} MessageReceived {1} queueLength={2}, hasWaitingChannel={3},  hasWaitingBatchChannel={4}",
                        prefix, msgId, incomingMessages.Count, hasWaitingChannel, hasWaitingBatchChannel)

                    if rawMessage.CheckSumValid then
                        if msgId |> acksGroupingTracker.IsDuplicate |> not then
                            let! schemaDecodeFunction = getSchemaDecodeFunction rawMessage.Metadata
                            let isChunked = rawMessage.Metadata.NumChunks > 1 && consumerConfig.SubscriptionType <> SubscriptionType.Shared
                            match decryptMessage rawMessage with
                            | Ok decryptedMessage ->
                                if decryptedMessage.Payload.Length <= clientCnx.MaxMessageSize then
                                    match decompressMessage decryptedMessage isChunked with
                                    | Ok decompressedMessage ->
                                        do! tryHandleMessagePayload
                                                decompressedMessage msgId hasWaitingChannel hasWaitingBatchChannel false
                                                isChunked schemaDecodeFunction clientCnx
                                    | Error _ ->
                                        do! discardCorruptedMessage msgId clientCnx CommandAck.ValidationError.DecompressionError
                                else
                                    do! discardCorruptedMessage msgId clientCnx CommandAck.ValidationError.UncompressedSizeCorruption
                            | Error _ ->
                                match consumerConfig.ConsumerCryptoFailureAction with
                                | ConsumerCryptoFailureAction.CONSUME ->
                                    Log.Logger.LogWarning("{0} {1} Decryption failed. Consuming encrypted message.",
                                                          prefix, msgId)
                                    do! tryHandleMessagePayload
                                            rawMessage msgId hasWaitingChannel hasWaitingBatchChannel true
                                            isChunked schemaDecodeFunction clientCnx
                                | ConsumerCryptoFailureAction.DISCARD ->
                                    Log.Logger.LogWarning("{0} {1}. Decryption failed. Discarding encrypted message.",
                                                          prefix, msgId)
                                    do! discardCorruptedMessage msgId clientCnx CommandAck.ValidationError.DecryptionError
                                | ConsumerCryptoFailureAction.FAIL ->
                                    Log.Logger.LogError("{0} {1}. Decryption failed. Failing encrypted message.",
                                                            prefix, msgId)
                                    unAckedMessageTracker.Add msgId |> ignore
                                | _ ->
                                    failwith "Unknown ConsumerCryptoFailureAction"
                        else
                            Log.Logger.LogWarning("{0} Ignoring message as it was already being acked earlier by same consumer {1}", prefix, msgId)
                            increaseAvailablePermits rawMessage.Metadata.NumMessages
                    else
                        do! discardCorruptedMessage msgId clientCnx CommandAck.ValidationError.ChecksumMismatch
                    return! loop ()

                | ConsumerMessage.Receive (cancellationToken, ch) ->

                    Log.Logger.LogDebug("{0} Receive", prefix)
                    if cancellationToken.IsCancellationRequested then
                        TaskCanceledException() :> exn |> Error |> ch.Reply
                    else
                        if incomingMessages.Count > 0 then
                            replyWithMessage ch <| dequeueMessage()
                        else
                            let tokenRegistration =
                                if cancellationToken.CanBeCanceled then
                                    let rec cancellationTokenRegistration =
                                        cancellationToken.Register((fun () ->
                                            Log.Logger.LogDebug("{0} receive cancelled", prefix)
                                            TaskCanceledException() :> exn |> Error |> ch.Reply
                                            this.Mb.Post(RemoveWaiter(cancellationTokenRegistration, ch))
                                        ), false) |> Some
                                    cancellationTokenRegistration
                                else
                                    None
                            waiters.AddLast((tokenRegistration, ch)) |> ignore
                            Log.Logger.LogDebug("{0} Receive waiting", prefix)
                    return! loop ()
                
                | ConsumerMessage.Acknowledge (messageId, ackType, txnOption, chOption) ->

                    Log.Logger.LogDebug("{0} Acknowledge {1} {2} {3}", prefix, messageId, ackType, txnOption)
                    let task = trySendAcknowledge ackType EmptyProperties txnOption messageId
                    chOption |> Option.iter (fun ch -> ch.Reply(task))
                    return! loop ()
                    
                | ConsumerMessage.BatchReceive (cancellationToken, ch) ->

                    Log.Logger.LogDebug("{0} BatchReceive", prefix)
                    if cancellationToken.IsCancellationRequested then
                        TaskCanceledException() :> exn |> Error |> ch.Reply
                    else
                        if batchWaiters.Count = 0 && hasEnoughMessagesForBatchReceive() then
                            replyWithBatch ch
                        else
                            let batchCts = new CancellationTokenSource()
                            let registration =
                                if cancellationToken.CanBeCanceled then
                                    let rec cancellationTokenRegistration =
                                        cancellationToken.Register((fun () ->
                                            Log.Logger.LogDebug("{0} batch receive cancelled", prefix)
                                            batchCts.Cancel()
                                            TaskCanceledException() :> exn |> Error |> ch.Reply
                                            this.Mb.Post(RemoveBatchWaiter(batchCts, cancellationTokenRegistration, ch))
                                        ), false) |> Some
                                    cancellationTokenRegistration
                                else
                                    None
                            batchWaiters.AddLast((batchCts, registration, ch)) |> ignore
                            asyncDelay
                                consumerConfig.BatchReceivePolicy.Timeout
                                (fun () ->
                                    if not batchCts.IsCancellationRequested then
                                        this.Mb.Post(SendBatchByTimeout)
                                    else
                                        batchCts.Dispose())
                            Log.Logger.LogDebug("{0} BatchReceive waiting", prefix)
                    return! loop ()
                    
                | ConsumerMessage.SendBatchByTimeout ->
                    
                    Log.Logger.LogDebug("{0} SendBatchByTimeout", prefix)
                    if batchWaiters.Count > 0 then
                        let ch = batchWaiters |> dequeueBatchWaiter
                        replyWithBatch ch
                    return! loop ()

                    
                | ConsumerMessage.NegativeAcknowledge messageId ->

                    Log.Logger.LogDebug("{0} NegativeAcknowledge {1}", prefix, messageId)
                    negativeAcksTracker.Add(messageId) |> ignore
                    // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
                    untrackMessage messageId
                    return! loop ()
                                    
                | ConsumerMessage.RemoveWaiter waiter ->
                    
                    waiters.Remove(waiter) |> ignore
                    let (ctrOpt, _) = waiter
                    ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
                    return! loop ()
                    
                | ConsumerMessage.RemoveBatchWaiter batchWaiter ->
                    
                    batchWaiters.Remove(batchWaiter) |> ignore
                    let (cts, ctrOpt, _) = batchWaiter
                    ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
                    cts.Dispose()
                    return! loop ()
                    
                | ConsumerMessage.AckReceipt reqId ->
                    
                    match ackRequests.TryGetValue(reqId) with
                    | true, (msgId, txnId, tcs) ->
                        ackRequests.Remove(reqId) |> ignore
                        tcs.SetResult()
                        Log.Logger.LogDebug("{0} MessageId : {1} has ack receipt by TxnId : {2}", prefix, msgId, txnId)
                    | _ ->
                        Log.Logger.LogInformation("{0} Ack request has been handled requestId : {1}", prefix, reqId)
                    return! loop ()
                    
                | ConsumerMessage.AckError (reqId, ex) ->
                    
                    match ackRequests.TryGetValue(reqId) with
                    | true, (msgId, txnId, tcs) ->
                        ackRequests.Remove(reqId) |> ignore
                        stats.IncrementNumAcksFailed()
                        tcs.SetException(ex)
                        Log.Logger.LogWarning("{0} MessageId : {1} has ack error by TxnId : {2}", prefix, msgId, txnId)
                    | _ ->
                        Log.Logger.LogInformation("{0} Ack request has been handled requestId : {1}", prefix, reqId)
                    return! loop ()

                | ConsumerMessage.RedeliverUnacknowledged (messageIds, channel) ->

                    Log.Logger.LogDebug("{0} RedeliverUnacknowledged", prefix)
                    match consumerConfig.SubscriptionType with
                    | SubscriptionType.Shared | SubscriptionType.KeyShared ->
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let messagesFromQueue = removeExpiredMessagesFromQueue messageIds
                            let batches = messageIds |> Seq.chunkBySize MAX_REDELIVER_UNACKNOWLEDGED
                            for ids in batches do
                                do!
                                    task {
                                        let! messageIdData = getRedeliveryMessageIdData ids
                                        if messageIdData.Length > 0 then
                                            let command = Commands.newRedeliverUnacknowledgedMessages consumerId (Some messageIdData)
                                            let! success = clientCnx.Send command
                                            if success then
                                                Log.Logger.LogDebug("{0} RedeliverAcknowledged complete", prefix)
                                            else
                                                Log.Logger.LogWarning("{0} RedeliverAcknowledged was not complete", prefix)
                                        else
                                            Log.Logger.LogDebug("{0} All messages were dead", prefix)
                                    } |> Async.AwaitTask
                            if messagesFromQueue > 0 then
                                increaseAvailablePermits messagesFromQueue
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                        channel.Reply()
                    | _ ->
                        this.Mb.Post(RedeliverAllUnacknowledged channel)
                        Log.Logger.LogInformation("{0} We cannot redeliver single messages if subscription type is not Shared", prefix)
                    return! loop ()

                | ConsumerMessage.RedeliverAllUnacknowledged channel ->

                    Log.Logger.LogDebug("{0} RedeliverAllUnacknowledged", prefix)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        let command = Commands.newRedeliverUnacknowledgedMessages consumerId None
                        let! success = clientCnx.Send command
                        if success then
                            let currentSize = incomingMessages.Count
                            if currentSize > 0 then
                                incomingMessages.Clear()
                                increaseAvailablePermits currentSize
                                unAckedMessageTracker.Clear()
                            Log.Logger.LogDebug("{0} RedeliverAllUnacknowledged complete", prefix)
                        else
                            Log.Logger.LogWarning("{0} RedeliverAllUnacknowledged was not complete", prefix)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    channel.Reply()
                    return! loop ()

                | ConsumerMessage.SeekAsync (seekData, channel) ->

                    Log.Logger.LogDebug("{0} SeekAsync", prefix)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogInformation("{0} Seek subscription to {1}", prefix, seekData)
                        try
                            let (payload, lastMessage) =
                                match seekData with
                                | Timestamp timestamp -> Commands.newSeekByTimestamp consumerId requestId timestamp, MessageId.Earliest
                                | MessageId messageId -> Commands.newSeekByMsgId consumerId requestId messageId, messageId
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            
                            duringSeek <- Some lastMessage
                            lastDequeuedMessageId <- MessageId.Earliest
                            
                            acksGroupingTracker.FlushAndClean()
                            incomingMessages.Clear()
                            Log.Logger.LogInformation("{0} Successfully reset subscription to {1}", prefix, seekData)
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} Failed to reset subscription to {1}", prefix, seekData)
                            channel.Reply <| Error ex
                    | _ ->
                        channel.Reply <| Error ((NotConnectedException "Not connected to broker") :> exn)
                        Log.Logger.LogError("{0} not connected, skipping SeekAsync {1}", prefix, seekData)
                    return! loop ()

                | ConsumerMessage.ReachedEndOfTheTopic ->

                    Log.Logger.LogWarning("{0} ReachedEndOfTheTopic", prefix)
                    hasReachedEndOfTopic <- true

                | ConsumerMessage.HasMessageAvailable channel ->

                    Log.Logger.LogDebug("{0} HasMessageAvailable", prefix)
                    
                    // avoid null reference
                    let startMessageId = startMessageId |> Option.defaultValue lastDequeuedMessageId
                    
                    // we haven't read yet. use startMessageId for comparison
                    if lastDequeuedMessageId = MessageId.Earliest then
                        // if we are starting from latest, we should seek to the actual last message first.
                        // allow the last one to be read when read head inclusively.
                        if startMessageId = MessageId.Latest then
                            task {
                                let! lastMessageIdResult = getLastMessageIdAsync() |> Async.AwaitTask
                                let lastMessageId = lastMessageIdResult.LastMessageId
                                // if the consumer is configured to read inclusive then we need to seek to the last message
                                if consumerConfig.ResetIncludeHead then
                                    let! result = this.Mb.PostAndAsyncReply(fun channel ->
                                        SeekAsync (MessageId lastMessageId, channel))
                                    return
                                        match result with
                                        | Ok () -> ()
                                        | Error ex -> reraize ex
                                match lastMessageIdResult.MarkDeletePosition with
                                | Some markDeletePosition ->
                                    if lastMessageId.EntryId < %0L then
                                        return false
                                    else
                                        // we only care about comparing ledger ids and entry ids as mark delete position doesn't have other ids such as batch index
                                        let result =
                                            match markDeletePosition.LedgerId, lastMessageId.LedgerId with
                                            | x, y when x > y -> 1
                                            | x, y when x < y -> -1
                                            | _ ->
                                                match markDeletePosition.EntryId, lastMessageId.EntryId with
                                                | x, y when x > y -> 1
                                                | x, y when x < y -> -1
                                                | _ -> 0
                                        return
                                            if consumerConfig.ResetIncludeHead then
                                                result <= 0
                                            else
                                                result < 0
                                | None ->
                                    if lastMessageId.EntryId < %0L then
                                        return false
                                    else
                                        return consumerConfig.ResetIncludeHead
                            } |> channel.Reply
                        elif hasMoreMessages this.LastMessageIdInBroker startMessageId consumerConfig.ResetIncludeHead then
                            channel.Reply(Task.FromResult(true))
                        else
                            task {
                                let! lastMessageIdResult = getLastMessageIdAsync()
                                this.LastMessageIdInBroker <- lastMessageIdResult.LastMessageId // Concurrent update - handle wisely
                                return hasMoreMessages this.LastMessageIdInBroker startMessageId consumerConfig.ResetIncludeHead
                            } |> channel.Reply

                    else
                        // read before, use lastDequeueMessage for comparison
                        if hasMoreMessages this.LastMessageIdInBroker lastDequeuedMessageId false then
                            channel.Reply(Task.FromResult(true))
                        else
                            task {
                                let! lastMessageIdResult = getLastMessageIdAsync()
                                this.LastMessageIdInBroker <- lastMessageIdResult.LastMessageId // Concurrent update - handle wisely
                                return hasMoreMessages this.LastMessageIdInBroker lastDequeuedMessageId false
                            } |> channel.Reply

                    return! loop ()
                                    
                | ConsumerMessage.ActiveConsumerChanged isActive ->
                    
                    Log.Logger.LogInformation("{0} ActiveConsumerChanged isActive={1}", prefix, isActive)
                    return! loop ()
                        
                | ConsumerMessage.Tick tick ->
                    
                    match tick with
                    | StatTick ->
                        stats.TickTime(incomingMessages.Count)
                    | ChunkTick ->
                        chunkedMessageTracker.RemoveExpireIncompleteChunkedMessages()
                    return! loop()
                    
                | ConsumerMessage.GetStats channel ->
                    
                    channel.Reply <| stats.GetStats()
                    return! loop ()
                    
                | ConsumerMessage.ReconsumeLater (msg, ackType, deliverAt, channel) ->
                    
                    let acknowldege = trySendAcknowledge ackType EmptyProperties None
                    task {
                        try
                            do! deadLettersProcessor.ReconsumeLater(msg, deliverAt, acknowldege)
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "Send to retry topic exception with topic: {0}, messageId: {1}",
                                            deadLettersProcessor.TopicName, msg.MessageId)
                            untrackMessage msg.MessageId
                            let redeliverSet = RedeliverSet()
                            redeliverSet.Add(msg.MessageId) |> ignore
                            redeliverMessages redeliverSet
                    } |> channel.Reply
                    return! loop ()
                    
                | ConsumerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveConsumer(consumerId)
                    return! loop ()

                | ConsumerMessage.ConnectionFailed ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    let nonRetriableError = ex |> PulsarClientException.isRetriableError |> not
                    let timeout = DateTime.Now > subscribeTimeout
                    if ((nonRetriableError || timeout) && subscribeTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed {1}", prefix,
                                                  if nonRetriableError then "with unretriableError" else "after timeout")
                        connectionHandler.Failed()
                        stopConsumer()
                    else
                        return! loop ()
                    
                | ClearIncomingMessagesAndGetMessageNumber ch ->
                    
                    Log.Logger.LogDebug("{0} ClearIncomingMessagesAndGetMessageNumber", prefix)
                    let messageNumber = incomingMessages.Count
                    incomingMessages.Clear()
                    incomingMessagesSize <- 0L
                    unAckedMessageTracker.Clear()
                    ch.Reply messageNumber
                    return! loop ()
                    
                | IncreaseAvailablePermits permits ->
                    
                    Log.Logger.LogDebug("{0} IncreaseAvailablePermits {1}", prefix, permits)
                    increaseAvailablePermits permits
                    return! loop ()
                    
                | ConsumerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseConsumer consumerId requestId
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            clientCnx.RemoveConsumer(consumerId)
                            connectionHandler.Closed()
                            stopConsumer()
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} failed to send close to server, doing local close", prefix)
                            connectionHandler.Closed()
                            stopConsumer()
                            channel.Reply <| Ok()
                    | _ ->
                        Log.Logger.LogWarning("{0} closing but current state {1}, doing local close", prefix, connectionHandler.ConnectionState)
                        connectionHandler.Closed()
                        stopConsumer()
                        channel.Reply <| Ok()

                | ConsumerMessage.Unsubscribe channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        unAckedMessageTracker.Close()
                        clearDeadLetters()
                        Log.Logger.LogInformation("{0} starting unsubscribe ", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newUnsubscribeConsumer consumerId requestId                       
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            clientCnx.RemoveConsumer(consumerId)
                            connectionHandler.Closed()
                            stopConsumer()
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} failed to unsubscribe", prefix)
                            channel.Reply <| Error ex
                            
                        if connectionHandler.ConnectionState <> Closed then
                            return! loop ()
                    | _ ->
                        Log.Logger.LogError("{0} can't unsubscribe since not connected", prefix)
                        channel.Reply <| Error((NotConnectedException "Not connected to broker") :> exn)
                        return! loop ()
            }
        loop ()
    )
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do startStatTimer()
    do startChunkTimer()

    abstract member ReceiveIndividualMessagesFromBatch: RawMessage -> (byte [] -> 'T) -> unit
    default this.ReceiveIndividualMessagesFromBatch (rawMessage: RawMessage) schemaDecodeFunction =
        let batchSize = rawMessage.Metadata.NumMessages
        let acker = BatchMessageAcker(batchSize)
        let mutable skippedMessages = 0
        use stream = new MemoryStream(rawMessage.Payload)
        use binaryReader = new BinaryReader(stream)
        for i in 0..batchSize-1 do
            Log.Logger.LogDebug("{0} processing message num - {1} in batch", prefix, i)
            let singleMessageMetadata = Serializer.DeserializeWithLengthPrefix<SingleMessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
            let singleMessagePayload = binaryReader.ReadBytes(singleMessageMetadata.PayloadSize)

            if duringSeek.IsSome || (isSameEntry(rawMessage.MessageId) && isPriorBatchIndex(%i)) then
                Log.Logger.LogDebug("{0} Ignoring message from before the startMessageId: {1} in batch", prefix, startMessageId)
                skippedMessages <- skippedMessages + 1
            elif singleMessageMetadata.CompactedOut then
                // message has been compacted out, so don't send to the user
                skippedMessages <- skippedMessages + 1
            elif rawMessage.AckSet.Count > 0 && not rawMessage.AckSet.[i] then
                skippedMessages <- skippedMessages + 1
            else
                let messageId =
                    {
                        rawMessage.MessageId with
                            Partition = partitionIndex
                            Type = Batch(%i, acker)
                            TopicName = %""
                    }
                let msgKey = singleMessageMetadata.PartitionKey
                let getValue () =
                    keyValueProcessor
                    |> Option.map (fun kvp -> kvp.DecodeKeyValue(msgKey, singleMessagePayload) :?> 'T)
                    |> Option.defaultWith (fun() -> schemaDecodeFunction singleMessagePayload)
                let properties =
                    if singleMessageMetadata.Properties.Count > 0 then
                                singleMessageMetadata.Properties
                                |> Seq.map (fun prop -> (prop.Key, prop.Value))
                                |> readOnlyDict
                            else
                                EmptyProps
                let eventTime =
                    if (singleMessageMetadata.ShouldSerializeEventTime()) then
                        Nullable(%(int64 singleMessageMetadata.EventTime))
                    else
                        Nullable()
                let message = Message (
                                messageId,
                                singleMessagePayload,
                                %msgKey,
                                singleMessageMetadata.PartitionKeyB64Encoded,
                                properties,
                                EncryptionContext.FromMetadata rawMessage.Metadata,
                                getSchemaVersionBytes rawMessage.Metadata.SchemaVersion,
                                %(int64 singleMessageMetadata.SequenceId),
                                singleMessageMetadata.OrderingKey,
                                rawMessage.Metadata.PublishTime,
                                eventTime,
                                getValue
                            )
                if (rawMessage.RedeliveryCount >= deadLettersProcessor.MaxRedeliveryCount) then
                    deadLettersProcessor.AddMessage(messageId, message)
                enqueueMessage message

        if skippedMessages > 0 then
            increaseAvailablePermits skippedMessages
    
    abstract member ConsumerIsReconnectedToBroker: unit -> unit
    default this.ConsumerIsReconnectedToBroker() =
        Log.Logger.LogInformation("{0} subscribed to topic {1}", prefix, topicName)
        avalablePermits <- 0
    
    member internal this.SendFlowPermits numMessages =
        async {
            Log.Logger.LogDebug("{0} SendFlowPermits {1}", prefix, numMessages)
            match connectionHandler.ConnectionState with
            | Ready clientCnx ->
                let flowCommand = Commands.newFlow consumerId numMessages
                let! success = clientCnx.Send flowCommand
                if not success then
                    Log.Logger.LogWarning("{0} failed SendFlowPermits {1}", prefix, numMessages)
            | _ ->
                Log.Logger.LogWarning("{0} not connected, skipping SendFlowPermits {1}", prefix, numMessages)
        } |> Async.StartImmediate
        
    member internal this.Waiters with get() = waiters
    
    member internal this.Mb with get(): MailboxProcessor<ConsumerMessage<'T>> = mb

    member this.ConsumerId with get() = consumerId

    member this.HasMessageAvailableAsync() =
        task {
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let! result = mb.PostAndAsyncReply(fun channel -> HasMessageAvailable channel)
            return! result
        }

    member this.LastMessageIdInBroker
        with get() = Volatile.Read(&lastMessageIdInBroker)
        and private set(value) = Volatile.Write(&lastMessageIdInBroker, value)
    
    override this.Equals consumer =
        consumerId = (consumer :?> IConsumer<'T>).ConsumerId

    override this.GetHashCode () = int consumerId
    
    member this.InitInternal() =
        task {
            do connectionHandler.GrabCnx()
            return! subscribeTsc.Task
        }

    static member Init(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration,
                       topicName: TopicName, connectionPool: ConnectionPool, partitionIndex: int,
                       hasParent: bool, startMessageId: MessageId option, startMessageRollbackDuration: TimeSpan, lookup: BinaryLookupService,
                       createTopicIfDoesNotExist: bool, schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option,
                       interceptors: ConsumerInterceptors<'T>, cleanup: ConsumerImpl<'T> -> unit) =
        task {
            let consumer =
                if consumerConfig.ReceiverQueueSize > 0 then
                    ConsumerImpl(consumerConfig, clientConfig, topicName, connectionPool, partitionIndex, hasParent,
                                startMessageId, startMessageRollbackDuration, lookup, createTopicIfDoesNotExist,
                                schema, schemaProvider, interceptors, cleanup)
                else
                    ZeroQueueConsumerImpl(consumerConfig, clientConfig, topicName, connectionPool, partitionIndex, hasParent,
                                        startMessageId, lookup, createTopicIfDoesNotExist,
                                        schema, schemaProvider, interceptors, cleanup) :> ConsumerImpl<'T>

            do! consumer.InitInternal()
            return consumer
        }

    abstract member ReceiveFsharpAsync: CancellationToken -> Async<ResultOrException<Message<'T>>>
    default this.ReceiveFsharpAsync(cancellationToken: CancellationToken) =
        async {
            connectionHandler.CheckIfActive() |> throwIfNotNull
            let! msgResult = mb.PostAndAsyncReply(fun channel -> Receive(cancellationToken, channel))
            match msgResult with
            | Ok _ ->
                return msgResult
            | Error _ ->
                stats.IncrementNumReceiveFailed()
                return msgResult
        }
    member internal this.RedeliverUnacknowledged msgIds =
        mb.PostAndAsyncReply(fun channel -> RedeliverUnacknowledged(msgIds, channel))
    
    interface IConsumer<'T> with
        
        member this.ReceiveAsync(cancellationToken: CancellationToken) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                match! mb.PostAndAsyncReply(fun channel -> Receive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    stats.IncrementNumReceiveFailed()
                    return reraize exn
            }
            
        member this.ReceiveAsync() =
            _this.ReceiveAsync(CancellationToken.None)

        member this.BatchReceiveAsync(cancellationToken: CancellationToken) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                match! mb.PostAndAsyncReply(fun channel -> BatchReceive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    stats.IncrementNumBatchReceiveFailed()
                    return reraize exn
            }
            
        member this.BatchReceiveAsync() =
            _this.BatchReceiveAsync(CancellationToken.None)

        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledge(this, msgId, exn)
                    raise exn

                let! task = mb.PostAndAsyncReply(fun ch -> Acknowledge(msgId, Individual, None, Some ch))
                // no need to IncrementNumAcksFailed, since ack can't fail
                return! task
            }
            
        member this.AcknowledgeAsync (msgId: MessageId, txn: Transaction) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledge(this, msgId, exn)
                    raise exn
                
                if txn |> isNull |> not then
                    do! txn.RegisterAckedTopic(topicName.CompleteTopicName, consumerConfig.SubscriptionName)
                    
                let! task = mb.PostAndAsyncReply(fun ch -> Acknowledge(msgId, Individual, Some txn, Some ch))
                if (not task.IsFaulted) && (txn |> isNull |> not) then
                    txn.RegisterAckOp(task)
                return! task
            }
            
        member this.AcknowledgeAsync (msgs: Messages<'T>) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    for msg in msgs do
                        stats.IncrementNumAcksFailed()
                        interceptors.OnAcknowledge(this, msg.MessageId, exn)
                    raise exn

                for msg in msgs do
                    mb.Post(Acknowledge(msg.MessageId, Individual, None, None))
            }
            
        member this.AcknowledgeAsync (msgIds: MessageId seq) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    for msgId in msgIds do
                        stats.IncrementNumAcksFailed()
                        interceptors.OnAcknowledge(this, msgId, exn)
                    raise exn

                for msgId in msgIds do
                    mb.Post(Acknowledge(msgId, AckType.Individual, None, None))
            }

        member this.AcknowledgeCumulativeAsync (msgId: MessageId) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledgeCumulative(this, msgId, exn)
                    raise exn

                let! task = mb.PostAndAsyncReply(fun ch -> Acknowledge(msgId, AckType.Cumulative, None, Some ch))
                // no need to IncrementNumAcksFailed, since ack can't fail
                return! task
            }
            
        member this.AcknowledgeCumulativeAsync (msgId: MessageId, txn: Transaction) =
            task {
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledgeCumulative(this, msgId, exn)
                    raise exn
                
                if txn |> isNull |> not then
                    txn.RegisterCumulativeAckConsumer(consumerId, consumerTxnOperations)
                    do! txn.RegisterAckedTopic(topicName.CompleteTopicName, consumerConfig.SubscriptionName)
                    
                let! task = mb.PostAndAsyncReply(fun ch -> Acknowledge(msgId, Cumulative, Some txn, Some ch))
                if (not task.IsFaulted) && (txn |> isNull |> not) then
                    txn.RegisterAckOp(task)
                return! task
            }

        member this.RedeliverUnacknowledgedMessagesAsync () =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                do! mb.PostAndAsyncReply(RedeliverAllUnacknowledged)
            }

        member this.SeekAsync (messageId: MessageId) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                return! wrapPostAndReply <| mb.PostAndAsyncReply(fun channel -> SeekAsync (MessageId messageId, channel))
            }

        member this.SeekAsync (timestamp: TimeStamp) =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                return! wrapPostAndReply <| mb.PostAndAsyncReply(fun channel -> SeekAsync (Timestamp timestamp, channel))
            }
            
        member this.SeekAsync (resolver: Func<string, SeekType>) : Task<Unit>  =
            let startFrom = resolver.Invoke %topicName.CompleteTopicName
            task {
                return! wrapPostAndReply <| mb.PostAndAsyncReply(fun channel -> SeekAsync (startFrom, channel))
            }
                        
        member this.GetLastMessageIdAsync () =
            task {
                let! result = getLastMessageIdAsync()
                return result.LastMessageId
            }

        member this.UnsubscribeAsync() =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                return! wrapPostAndReply <| mb.PostAndAsyncReply(ConsumerMessage.Unsubscribe)
            }

        member this.HasReachedEndOfTopic = hasReachedEndOfTopic

        member this.NegativeAcknowledge msgId =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull
                mb.Post(NegativeAcknowledge(msgId))
            }

        member this.NegativeAcknowledge (msgs: Messages<'T>)  =
            task {
                connectionHandler.CheckIfActive() |> throwIfNotNull 
                for msg in msgs do
                    mb.Post(NegativeAcknowledge(msg.MessageId))
            }
        
        member this.ConsumerId = consumerId

        member this.Topic = %topicName.CompleteTopicName

        member this.Name = consumerName

        member this.GetStatsAsync() =
            task {
                return! 
                    match connectionHandler.ConnectionState with
                    | Closed | Closing -> raise <| AlreadyClosedException(prefix + "already closed")
                    | _ -> mb.PostAndAsyncReply(ConsumerMessage.GetStats)
            }
            
        member this.ReconsumeLaterAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledge(this, msg.MessageId, exn)
                    raise exn
                let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLater(msg, Individual, deliverAt, channel))
                return! result
            }
            
        member this.ReconsumeLaterCumulativeAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    stats.IncrementNumAcksFailed()
                    interceptors.OnAcknowledgeCumulative(this, msg.MessageId, exn)
                    raise exn
                let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLater(msg, AckType.Cumulative, deliverAt, channel))
                return! result
            }
        
        member this.ReconsumeLaterAsync (msgs: Messages<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let exn = connectionHandler.CheckIfActive()
                if not (isNull exn) then
                    for msg in msgs do
                        stats.IncrementNumAcksFailed()
                        interceptors.OnAcknowledge(this, msg.MessageId, exn)
                    raise exn
                for msg in msgs do
                    let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLater(msg, Individual, deliverAt, channel))
                    do! result
            }
            
        member this.LastDisconnectedTimestamp =
            connectionHandler.LastDisconnectedTimestamp
            
        
    interface IAsyncDisposable with
        
        member this.DisposeAsync() =     
            match connectionHandler.ConnectionState with
            | Closing | Closed ->
                ValueTask()
            | _ ->
                task {
                    let! result = mb.PostAndAsyncReply(ConsumerMessage.Close)
                    match result with
                    | Ok () -> ()
                    | Error ex -> reraize ex 
                } |> ValueTask


and internal ZeroQueueConsumerImpl<'T> (consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration,
                           topicName: TopicName, connectionPool: ConnectionPool, partitionIndex: int,
                           hasParent: bool, startMessageId: MessageId option, 
                           lookup: BinaryLookupService,
                           createTopicIfDoesNotExist: bool, schema: ISchema<'T>,
                           schemaProvider: MultiVersionSchemaInfoProvider option,
                           interceptors: ConsumerInterceptors<'T>, cleanup: ConsumerImpl<'T> -> unit) as this =
    
    inherit ConsumerImpl<'T>(consumerConfig, clientConfig, topicName, connectionPool, partitionIndex, hasParent,
                             startMessageId, TimeSpan.Zero, lookup, createTopicIfDoesNotExist,
                             schema, schemaProvider, interceptors, cleanup)

    let _this = this :> IConsumer<'T>

    let prefix = $"consumer({this.ConsumerId}, {_this.Name}, {partitionIndex})"
    
    override this.ConsumerIsReconnectedToBroker() =
        base.ConsumerIsReconnectedToBroker()
        if this.Waiters.Count > 0 then
            this.SendFlowPermits this.Waiters.Count
        
    override this.ReceiveIndividualMessagesFromBatch (_: RawMessage) _ =
        Log.Logger.LogError("{0} Closing consumer due to unsupported received batch-message with zero receiver queue size", prefix)
        let _ = this.Mb.PostAndReply(ConsumerMessage.Close) 
        let exn = 
            $"Unsupported Batch message with 0 size receiver queue for [{consumerConfig.SubscriptionName}]-[{consumerConfig.ConsumerName}]"
            |> InvalidMessageException
        raise exn
        
    override this.ReceiveFsharpAsync(ct) =
        this.SendFlowPermits 1
        base.ReceiveFsharpAsync(ct) 
    
    interface IConsumer<'T> with
        override this.ReceiveAsync(cancellationToken: CancellationToken) =
            task {
                match! this.ReceiveFsharpAsync(cancellationToken) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    return reraize exn
            }
            
        override this.ReceiveAsync() =
            _this.ReceiveAsync(CancellationToken.None)
        
        override this.BatchReceiveAsync() =
            task {
                return raise (NotSupportedException "BatchReceiveAsync not supported.")
            }