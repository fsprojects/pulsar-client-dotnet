namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Generic
open System
open Pulsar.Client.Internal
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open System.Threading
open System.Timers

type internal MultiTopicConnectionState =
    | Uninitialized
    | Failed
    | Ready
    | Closing
    | Closed
    
type internal PatternInfo<'T> =
    {
        GetTopics: unit -> Task<TopicName[]>
        GetConsumerInfo: TopicName -> Task<ConsumerInitInfo<'T>>
        InitialTopics: ConsumerInitInfo<'T>[]
    }
    
type internal MultiConsumerType<'T> =
    | Partitioned of ConsumerInitInfo<'T>
    | MultiTopic of ConsumerInitInfo<'T>[]
    | Pattern of PatternInfo<'T>

type internal BatchAddResponse<'T> =
    | Expired
    | BatchReady of Messages<'T>
    | Success

type internal MultiTopicConsumerMessage<'T> =
    | Init
    | Receive of CancellationToken * AsyncReplyChannel<ResultOrException<Message<'T>>>
    | BatchReceive of CancellationToken * AsyncReplyChannel<ResultOrException<Messages<'T>>>
    | MessageReceived of ResultOrException<Message<'T>> * AsyncReplyChannel<unit>
    | SendBatchByTimeout
    | Acknowledge of AsyncReplyChannel<Task<unit>> * MessageId
    | NegativeAcknowledge of AsyncReplyChannel<Task<unit>> * MessageId
    | AcknowledgeCumulative of AsyncReplyChannel<Task<unit>> * MessageId
    | RedeliverUnacknowledged of RedeliverSet * AsyncReplyChannel<Task>
    | RedeliverAllUnacknowledged of AsyncReplyChannel<Task>
    | Close of AsyncReplyChannel<ResultOrException<unit>>
    | Unsubscribe of AsyncReplyChannel<ResultOrException<unit>>
    | HasReachedEndOfTheTopic of AsyncReplyChannel<bool>
    | Seek of AsyncReplyChannel<Task> * uint64
    | PatternTickTime
    | PartitionTickTime
    | GetStats of AsyncReplyChannel<Task<ConsumerStats array>>
    | ReconsumeLater of Message<'T> * int64 * AsyncReplyChannel<Task<unit>>
    | ReconsumeLaterCumulative of Message<'T> * int64 * AsyncReplyChannel<Task<unit>>
    | RemoveWaiter of Waiter<'T>
    | RemoveBatchWaiter of BatchWaiter<'T>

type internal TopicAndConsumer<'T> =
    {
        IsPartitioned: bool
        TopicName: TopicName
        Consumer: ConsumerImpl<'T>
    }

type internal MultiTopicsConsumerImpl<'T> private (consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                      multiConsumerType: MultiConsumerType<'T>, lookup: BinaryLookupService,
                                      interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) as this =

    let _this = this :> IConsumer<'T>
    let consumerId = Generators.getNextConsumerId()
    let prefix = sprintf "mt/consumer(%u, %s)" %consumerId consumerConfig.ConsumerName
    let consumers = Dictionary<CompleteTopicName,IConsumer<'T> * TaskGenerator<ResultOrException<Message<'T>>>>()
    let consumerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let pollerCts = new CancellationTokenSource()
    let mutable connectionState = MultiTopicConnectionState.Uninitialized
    let mutable currentStream = Unchecked.defaultof<TaskSeq<ResultOrException<Message<'T>>>>
    let partitionedTopics = Dictionary<TopicName, ConsumerInitInfo<'T>>()
    let allTopics = HashSet()
    let mutable incomingMessagesSize = 0L
    let defaultWaitingPoller = Unchecked.defaultof<AsyncReplyChannel<unit>>
    let mutable waitingPoller = defaultWaitingPoller
    let waiters = LinkedList<Waiter<'T>>()
    let batchWaiters = LinkedList<BatchWaiter<'T>>()
    let incomingMessages = Queue<ResultOrException<Message<'T>>>()
    let partitionsTimer = new Timer(60_000.0) // 1 minute
    let patternTimer = new Timer(consumerConfig.PatternAutoDiscoveryPeriod.TotalMilliseconds)
    let sharedQueueResumeThreshold = consumerConfig.ReceiverQueueSize / 2
    
    let redeliverMessages messages =
        task {
            let! result = this.Mb.PostAndAsyncReply(fun channel -> RedeliverUnacknowledged (messages, channel))
            return! result
        } |> ignore
    
    let unAckedMessageRedeliver messages =
        interceptors.OnAckTimeoutSend(this, messages)
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
    
    let statsReduce (statsArray: ConsumerStats array) =
        let mutable numMsgsReceived: int64 = 0L
        let mutable numBytesReceived: int64 = 0L
        let mutable numReceiveFailed: int64 = 0L
        // should be always 0 for multi-topics consumer
        let mutable numBatchReceiveFailed: int64 = 0L
        let mutable numAcksSent: int64 = 0L
        let mutable numAcksFailed: int64 = 0L
        let mutable totalMsgsReceived: int64 = 0L
        let mutable totalBytesReceived: int64 = 0L
        let mutable totalReceiveFailed: int64 = 0L
        // should be always 0 for multi-topics consumer
        let mutable totalBatchReceiveFailed: int64 = 0L
        let mutable totalAcksSent: int64 = 0L
        let mutable totalAcksFailed: int64 = 0L
        let mutable receivedMsgsRate: float = 0.0
        let mutable receivedBytesRate: float = 0.0
        let mutable intervalDurationSum: float = 0.0
        let mutable incomingMsgs: int = 0
        
        statsArray |> Array.iter(fun stats ->
            numMsgsReceived <- numMsgsReceived + stats.NumMsgsReceived
            numBytesReceived <- numBytesReceived + stats.NumBytesReceived
            numReceiveFailed <- numReceiveFailed + stats.NumReceiveFailed
            numBatchReceiveFailed <- numBatchReceiveFailed + stats.NumBatchReceiveFailed
            numAcksSent <- numAcksSent + stats.NumAcksSent
            numAcksFailed <- numAcksFailed + stats.NumAcksFailed
            totalMsgsReceived <- totalMsgsReceived + stats.TotalMsgsReceived
            totalBytesReceived <- totalBytesReceived + stats.TotalBytesReceived
            totalReceiveFailed <- totalReceiveFailed + stats.TotalReceiveFailed
            totalBatchReceiveFailed <- totalBatchReceiveFailed + stats.TotalBatchReceiveFailed
            totalAcksSent <- totalAcksSent + stats.TotalAcksSent
            totalAcksFailed <- totalAcksFailed + stats.TotalAcksFailed
            receivedMsgsRate <- receivedMsgsRate + stats.ReceivedMsgsRate
            receivedBytesRate <- receivedBytesRate + stats.ReceivedBytesRate
            intervalDurationSum <- intervalDurationSum + stats.IntervalDuration
            incomingMsgs <- incomingMsgs + stats.IncomingMsgs            
            )
        {
            NumMsgsReceived = numMsgsReceived
            NumBytesReceived = numBytesReceived
            NumReceiveFailed = numReceiveFailed
            NumBatchReceiveFailed = numBatchReceiveFailed
            NumAcksSent = numAcksSent
            NumAcksFailed = numAcksFailed
            TotalMsgsReceived = totalMsgsReceived
            TotalBytesReceived = totalBytesReceived
            TotalReceiveFailed = totalReceiveFailed
            TotalBatchReceiveFailed = totalBatchReceiveFailed
            TotalAcksSent = totalAcksSent
            TotalAcksFailed = totalAcksFailed
            ReceivedMsgsRate = receivedMsgsRate
            ReceivedBytesRate = receivedBytesRate
            IntervalDuration = if statsArray.Length > 0 then intervalDurationSum / float statsArray.Length else 0.0
            IncomingMsgs = incomingMsgs
        }
    
    let getStream (topic: CompleteTopicName) (consumer: ConsumerImpl<'T>) =
        Log.Logger.LogDebug("{0} getStream", topic)
        let consumerImp = consumer :> IConsumer<'T>
        fun () ->
            task {
                if consumerImp.HasReachedEndOfTopic then
                    Log.Logger.LogWarning("{0} topic was terminated", topic)
                    do! Task.Delay(Timeout.Infinite) // infinite delay for terminated topic
                let! message = consumer.ReceiveFsharpAsync(CancellationToken.None)
                return
                    message |> Result.map (fun msg ->
                        let newMessageId = { msg.MessageId with TopicName = topic }
                        msg.WithMessageId(newMessageId)
                    )
            }
        
    let stopConsumer() =
        cleanup(this)
        partitionsTimer.Close()
        patternTimer.Close()
        unAckedMessageTracker.Close()
        pollerCts.Cancel()
        pollerCts.Dispose()
        while waiters.Count > 0 do
            let waitingChannel = waiters |> ConsumerBase.dequeueWaiter
            waitingChannel.Reply(Error (AlreadyClosedException("Consumer is already closed") :> exn))
        while batchWaiters.Count > 0 do
            let cts, batchWaitingChannel = batchWaiters |> ConsumerBase.dequeueBatchWaiter
            batchWaitingChannel.Reply(Error (AlreadyClosedException("Consumer is already closed") :> exn))
            cts.Cancel()
            cts.Dispose()
        Log.Logger.LogInformation("{0} stopped", prefix)

    let singleInit (consumerInitInfo: ConsumerInitInfo<'T>) =
        let topic = consumerInitInfo.TopicName
        let numPartitions = consumerInitInfo.Metadata.Partitions
        let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / numPartitions)
        let consumersTasks = 
            Seq.init numPartitions (fun partitionIndex ->
                let partitionedTopic = topic.GetPartition(partitionIndex)
                let partititonedConfig = { consumerConfig with
                                            ReceiverQueueSize = receiverQueueSize
                                            Topics = seq { partitionedTopic } |> Seq.cache }
                task {
                    let! result =    
                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic, connectionPool, partitionIndex, true,
                                          None, lookup, true, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider, interceptors, fun _ -> ())
                    return { IsPartitioned = true; TopicName = partitionedTopic; Consumer = result }
                })
            |> Seq.cache
        task {
            let! consumerResults = consumersTasks |> Task.WhenAll
            allTopics.Add consumerInitInfo.TopicName |> ignore
            partitionedTopics.Add(topic, consumerInitInfo)
            return
                consumerResults
                |> Seq.map (fun topicAndConsumer ->
                    let stream = getStream topicAndConsumer.TopicName.CompleteTopicName topicAndConsumer.Consumer
                    consumers.Add(topicAndConsumer.TopicName.CompleteTopicName, (topicAndConsumer.Consumer :> IConsumer<'T>, stream))
                    stream
                    )
                |> Seq.cache
        }, consumersTasks
    
    let multiInit (consumerInitInfos: ConsumerInitInfo<'T>[]) createTopicIfDoesNotExist =
        
        if consumerInitInfos.Length > 0 then
            let mutable totalConsumersCount = consumers.Count
            let newPartitionedConsumers = ResizeArray()
            let newTopics = ResizeArray()
            for consumerInfo in consumerInitInfos do
                newTopics.Add(consumerInfo.TopicName)
                if consumerInfo.Metadata.IsMultiPartitioned then
                    totalConsumersCount <- totalConsumersCount + consumerInfo.Metadata.Partitions
                    newPartitionedConsumers.Add(consumerInfo)
                else
                    totalConsumersCount <- totalConsumersCount + 1
            let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / totalConsumersCount)
            let consumersTasks =
                consumerInitInfos
                |> Seq.collect
                    (fun consumerInitInfo ->
                        if consumerInitInfo.Metadata.IsMultiPartitioned then
                            let topic = consumerInitInfo.TopicName
                            let numPartitions = consumerInitInfo.Metadata.Partitions
                            Seq.init numPartitions (fun partitionIndex ->
                                let partitionedTopic = topic.GetPartition(partitionIndex)
                                let partititonedConfig = { consumerConfig with
                                                            ReceiverQueueSize = receiverQueueSize
                                                            Topics = seq { partitionedTopic } |> Seq.cache }
                                task {
                                    let! result =    
                                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic, connectionPool, partitionIndex, true,
                                                          None, lookup, createTopicIfDoesNotExist, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider, interceptors, fun _ -> ())
                                    return { IsPartitioned = true; TopicName = partitionedTopic; Consumer = result }
                                })
                        else
                            seq {
                                task {
                                    let partititonedConfig = { consumerConfig with
                                                                ReceiverQueueSize = receiverQueueSize
                                                                Topics = seq { consumerInitInfo.TopicName } |> Seq.cache }
                                    let! result =    
                                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic, connectionPool, -1, true,
                                                          None, lookup, createTopicIfDoesNotExist, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider, interceptors, fun _ -> ())
                                    return { IsPartitioned = false; TopicName = consumerInitInfo.TopicName; Consumer = result }
                                }
                            })
                |> Seq.cache
            task {
                let! consumerResults = consumersTasks |> Task.WhenAll
                allTopics.UnionWith newTopics
                for consumerInfo in newPartitionedConsumers do
                    partitionedTopics.Add(consumerInfo.TopicName, consumerInfo)
                return
                    consumerResults
                    |> Seq.map (fun topicAndConsumer ->
                        let stream = getStream topicAndConsumer.TopicName.CompleteTopicName topicAndConsumer.Consumer
                        consumers.Add(topicAndConsumer.TopicName.CompleteTopicName, (topicAndConsumer.Consumer :> IConsumer<'T>, stream))
                        stream)
                    |> Seq.cache
            }, consumersTasks
        else
            Task.FromResult(Seq.empty), Seq.empty
        
    let processAddedTopics (topicsToAdd: TopicName seq) (getConsumerInitInfo: TopicName -> Task<ConsumerInitInfo<'T>>) =
        task {
            let! consumerInfos =
                topicsToAdd
                |> Seq.map getConsumerInitInfo
                |> Task.WhenAll
            let addedTopicsTask, consumersTasks = multiInit consumerInfos false
            try
                return! addedTopicsTask
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} could not processAddedTopics", prefix)
                do! consumersTasks
                    |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                    |> Seq.map (fun t ->
                        let consumer = t.Result.Consumer
                        (consumer :> IConsumer<'T>).DisposeAsync().AsTask())
                    |> Task.WhenAll
                    |> Async.AwaitTask
                    |> Async.Ignore
                return Seq.empty
        }
        
    let processRemovedTopics (topicsToRemove: TopicName seq) =
        let consumersTasks =
            topicsToRemove
            |> Seq.collect(fun topicToRemove ->
                    consumers
                    |> Seq.filter(fun (KeyValue(topic, _)) ->
                        let t: string = %topic
                        let lastIndexOfPartition = t.LastIndexOf("-partition-")
                        topicToRemove.CompleteTopicName =
                            if lastIndexOfPartition > 0 then
                                %t.Substring(0, lastIndexOfPartition)
                            else
                                %t)
                    |> Seq.map(fun (KeyValue(topic, (consumer, stream))) -> task {
                        do! consumer.DisposeAsync()
                        return topicToRemove, topic, stream
                    })
                )
            |> Seq.cache
        task {
            try
                let! allRemovedTopics =
                    consumersTasks |> Task.WhenAll
                return
                    allRemovedTopics
                    |> Seq.map (fun (removedTopic, removedTopicPartition, stream) ->
                        consumers.Remove(removedTopicPartition) |> ignore
                        allTopics.Remove(removedTopic) |> ignore
                        partitionedTopics.Remove(removedTopic) |> ignore
                        stream)
                    |> Seq.cache
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} could not processRemovedTopics fully", prefix)
                return consumersTasks
                    |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                    |> Seq.map (fun t -> t.Result)
                    |> Seq.map (fun (removedTopic, removedTopicPartition, stream) ->
                        consumers.Remove(removedTopicPartition) |> ignore
                        allTopics.Remove(removedTopic) |> ignore
                        partitionedTopics.Remove(removedTopic) |> ignore
                        stream)
                    |> Seq.cache
        }    
   
    let isPollingAllowed() =
        incomingMessages.Count <= sharedQueueResumeThreshold
     
    let enqueueMessage (m: ResultOrException<Message<'T>>) =
        match m with
        | Ok msg -> incomingMessagesSize <- incomingMessagesSize + msg.Data.LongLength
        | _ -> ()
        incomingMessages.Enqueue(m)

    let dequeueMessage() =
        let m = incomingMessages.Dequeue()
        match m with
        | Ok msg -> incomingMessagesSize <- incomingMessagesSize - msg.Data.LongLength
        | _ -> ()
        if isPollingAllowed() && (waitingPoller <> defaultWaitingPoller) then
            waitingPoller.Reply()
            waitingPoller <- defaultWaitingPoller
        m
    
    let hasEnoughMessagesForBatchReceive() =
        ConsumerBase.hasEnoughMessagesForBatchReceive consumerConfig.BatchReceivePolicy incomingMessages.Count incomingMessagesSize
    
    let getNewPartitions () =
        task {
            try
                let! results =
                    partitionedTopics
                    |> Seq.map (fun (KeyValue(topic, _)) -> task {
                            let! partitionNames = lookup.GetPartitionsForTopic(topic)
                            return (topic, partitionNames) 
                        })
                    |> Task.WhenAll
                return Some results
            with ex ->
               Log.Logger.LogWarning(ex, "{0} Unabled to fetch new topics", prefix)
               return None
        }
        

    let replyWithBatch (cts: CancellationTokenSource option) (ch: AsyncReplyChannel<ResultOrException<Messages<'T>>>) =
        cts |> Option.iter (fun cts ->
            cts.Cancel()
            cts.Dispose()
        )
        let messages = Messages(consumerConfig.BatchReceivePolicy.MaxNumMessages, consumerConfig.BatchReceivePolicy.MaxNumBytes)
        
        let mutable shouldContinue = true
        let mutable error = None
        while shouldContinue && incomingMessages.Count > 0 do
            let m = incomingMessages.Peek()
            match m with
            | Ok msgPeeked ->
                if messages.CanAdd msgPeeked then
                    match dequeueMessage() with
                    | Ok msg ->
                        unAckedMessageTracker.Add msg.MessageId |> ignore
                        messages.Add msg
                    | _ -> failwith "Impossible branch in replyWithBatch"
                else
                    shouldContinue <- false
            | Error ex ->
                shouldContinue <- false
                error <- Some ex
        match error with
        | Some ex when messages.Count = 0 ->
            // only fail when no batched messages before error happened
            ch.Reply <| Error ex
        | _ ->
            Log.Logger.LogDebug("{0} BatchFormed with size {1}", prefix, messages.Size)
            ch.Reply <| Ok messages

    let replyWithMessage (channel: AsyncReplyChannel<ResultOrException<Message<'T>>>) (message: ResultOrException<Message<'T>>) =
        match message with
        | Ok msg -> unAckedMessageTracker.Add msg.MessageId |> ignore
        | _ -> ()
        channel.Reply message
    
    let runPoller (ct: CancellationToken) =
        Task.Run<unit>(fun () ->
                task {
                    while not ct.IsCancellationRequested do
                        let! msg = currentStream.Next()
                        if not ct.IsCancellationRequested then
                            do! this.Mb.PostAndAsyncReply(fun channel -> MessageReceived (msg, channel))
                        ()
                }
            , ct)
    
    
    let mb = MailboxProcessor<MultiTopicConsumerMessage<'T>>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Init ->
                    
                    Log.Logger.LogDebug("{0} Init", prefix)
                    let (newStreamsTask, consumersTasks) =
                        match multiConsumerType with
                        | Partitioned consumerInitInfo ->
                            singleInit consumerInitInfo
                        | MultiTopic consumerInitInfos ->
                            multiInit consumerInitInfos true
                        | Pattern patternInfo ->
                            multiInit patternInfo.InitialTopics false
                    try
                        let! streams =
                            newStreamsTask
                            |> Async.AwaitTask
                        this.ConnectionState <- Ready
                        Log.Logger.LogInformation("{0} created", prefix)
                        currentStream <- streams |> TaskSeq
                        runPoller pollerCts.Token |> ignore
                        consumerCreatedTsc.SetResult()
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} could not create", prefix)
                        do! consumersTasks
                            |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                            |> Seq.map (fun t ->
                                let consumer = t.Result.Consumer
                                (consumer :> IConsumer<'T>).DisposeAsync().AsTask())
                            |> Task.WhenAll
                            |> Async.AwaitTask
                            |> Async.Ignore
                        this.ConnectionState <- Failed
                        consumerCreatedTsc.SetException(ex)
                        stopConsumer()    
                    
                    if this.ConnectionState <> Failed then
                        return! loop ()
                        
                | MessageReceived (message, pollerChannel) ->
                    
                    let hasWaitingChannel = waiters.Count > 0
                    let hasWaitingBatchChannel = batchWaiters.Count > 0
                    Log.Logger.LogDebug("{0} MessageReceived queueLength={1}, hasWaitingChannel={2},  hasWaitingBatchChannel={3}",
                        prefix, incomingMessages.Count, hasWaitingChannel, hasWaitingBatchChannel)
                    // handle message
                    if hasWaitingChannel then
                        let waitingChannel = waiters |> ConsumerBase.dequeueWaiter
                        if (incomingMessages.Count = 0) then
                            replyWithMessage waitingChannel message
                        else
                            enqueueMessage message
                            replyWithMessage waitingChannel <| dequeueMessage()
                    else
                        enqueueMessage message
                        if hasWaitingBatchChannel && hasEnoughMessagesForBatchReceive() then
                            let cts, ch = batchWaiters |> ConsumerBase.dequeueBatchWaiter
                            replyWithBatch (Some cts) ch
                    // check if should reply to poller immediately
                    if isPollingAllowed() |> not then
                        waitingPoller <- pollerChannel
                    else
                        pollerChannel.Reply()
                    
                    return! loop ()
                        
                | Receive (cancellationToken, ch) ->
                    
                    Log.Logger.LogDebug("{0} Receive", prefix)
                    if incomingMessages.Count > 0 then
                        replyWithMessage ch <| dequeueMessage()
                    else
                        let rec cancellationTokenRegistration =
                            cancellationToken.Register((fun () ->
                                Log.Logger.LogDebug("{0} receive cancelled", prefix)
                                ch.Reply (TaskCanceledException() :> exn |> Error)
                                this.Mb.Post(RemoveWaiter(cancellationTokenRegistration, ch))
                            ), false)
                        waiters.AddLast((cancellationTokenRegistration, ch)) |> ignore
                        Log.Logger.LogDebug("{0} Receive waiting", prefix)
                    return! loop ()
                    
                | BatchReceive (cancellationToken, ch) ->
                    
                    Log.Logger.LogDebug("{0} BatchReceive", prefix)
                    if batchWaiters.Count = 0 && hasEnoughMessagesForBatchReceive() then
                        replyWithBatch None ch
                    else
                        let batchCts = new CancellationTokenSource()
                        let rec cancellationTokenRegistration =
                            cancellationToken.Register((fun () ->
                                Log.Logger.LogDebug("{0} batch receive cancelled", prefix)
                                batchCts.Cancel()
                                ch.Reply (TaskCanceledException() :> exn |> Error)
                                this.Mb.Post(RemoveBatchWaiter(batchCts, cancellationTokenRegistration, ch))
                            ), false)
                        batchWaiters.AddLast((batchCts, cancellationTokenRegistration, ch)) |> ignore
                        asyncDelay
                            consumerConfig.BatchReceivePolicy.Timeout
                            (fun () ->
                                if not batchCts.IsCancellationRequested then
                                    this.Mb.Post(SendBatchByTimeout)
                                else
                                    batchCts.Dispose())
                        Log.Logger.LogDebug("{0} BatchReceive waiting", prefix)
                    return! loop ()
                    
                | SendBatchByTimeout ->
                    
                    Log.Logger.LogDebug("{0} SendBatchByTimeout", prefix)
                    if batchWaiters.Count > 0 then
                        let cts, ch = batchWaiters |> ConsumerBase.dequeueBatchWaiter
                        replyWithBatch (Some cts) ch
                    return! loop ()
                
                | Acknowledge (channel, msgId) ->

                    Log.Logger.LogDebug("{0} Acknowledge {1}", prefix, msgId)
                    let (consumer, _) = consumers.[msgId.TopicName]
                    task {
                        do! consumer.AcknowledgeAsync(msgId)
                        unAckedMessageTracker.Remove msgId |> ignore
                    } |> channel.Reply
                    return! loop ()

                | NegativeAcknowledge (channel, msgId) ->

                    Log.Logger.LogDebug("{0} NegativeAcknowledge {1}", prefix, msgId)
                    let (consumer, _) = consumers.[msgId.TopicName]
                    task {
                        do! consumer.NegativeAcknowledge msgId
                        unAckedMessageTracker.Remove msgId |> ignore
                    } |> channel.Reply
                    return! loop ()

                | AcknowledgeCumulative (channel, msgId) ->

                    Log.Logger.LogDebug("{0} AcknowledgeCumulative {1}", prefix, msgId)
                    let (consumer, _) = consumers.[msgId.TopicName]
                    task {
                        do! consumer.AcknowledgeCumulativeAsync msgId
                        unAckedMessageTracker.RemoveMessagesTill msgId |> ignore
                    } |> channel.Reply
                    return! loop ()
                
                | RedeliverAllUnacknowledged channel ->

                    Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                    match this.ConnectionState with
                    | Ready ->
                        try
                            let! _ =
                                consumers
                                |> Seq.map(fun (KeyValue(_, (consumer, _))) -> consumer.RedeliverUnacknowledgedMessagesAsync())
                                |> Task.WhenAll
                                |> Async.AwaitTask
                            unAckedMessageTracker.Clear()
                            incomingMessages.Clear()
                            currentStream.RestartCompletedTasks()
                            incomingMessagesSize <- 0L
                            channel.Reply <| Task.FromResult()
                        with ex ->
                            Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                            channel.Reply <| Task.FromException ex
                    | _ ->
                        channel.Reply(Task.FromException(Exception(prefix + " invalid state: " + this.ConnectionState.ToString())))
                    return! loop ()
                    
                | RedeliverUnacknowledged (messageIds, channel) ->

                    Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                    match consumerConfig.SubscriptionType with
                    | SubscriptionType.Shared | SubscriptionType.KeyShared ->
                        match this.ConnectionState with
                        | Ready ->
                            try
                                let! _ =
                                    messageIds
                                    |> Seq.groupBy (fun msgId -> msgId.TopicName)
                                    |> Seq.map(fun (topicName, msgIds) ->
                                        let (consumer, _) = consumers.[topicName]
                                        msgIds |> RedeliverSet |> (consumer :?> ConsumerImpl<'T>).RedeliverUnacknowledged
                                        )
                                    |> Async.Parallel
                                    |> Async.Ignore
                                channel.Reply <| Task.FromResult()
                            with ex ->
                                Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                                channel.Reply <| Task.FromException ex
                        | _ ->
                            channel.Reply(Task.FromException <| Exception(prefix + " invalid state: " + this.ConnectionState.ToString()))
                    | _ ->
                        this.Mb.Post(RedeliverAllUnacknowledged channel)
                        Log.Logger.LogInformation("{0} We cannot redeliver single messages if subscription type is not Shared", prefix)
                    return! loop ()

                | RemoveWaiter waiter ->
                    
                    waiters.Remove(waiter) |> ignore
                    let (ctr, _) = waiter
                    ctr.Dispose()
                    return! loop ()
                    
                | RemoveBatchWaiter batchWaiter ->
                    
                    batchWaiters.Remove(batchWaiter) |> ignore
                    let (cts, ctr, _) = batchWaiter
                    ctr.Dispose()
                    cts.Dispose()
                    return! loop ()

                | HasReachedEndOfTheTopic channel ->

                    Log.Logger.LogDebug("{0} HasReachedEndOfTheTopic", prefix)
                    consumers
                    |> Seq.forall (fun (KeyValue(_, (consumer, _))) -> consumer.HasReachedEndOfTopic)
                    |> channel.Reply
                    return! loop ()

                | Seek (channel, ts) ->

                    Log.Logger.LogDebug("{0} Seek {1}", prefix, ts)
                    consumers
                    |> Seq.map (fun (KeyValue(_, (consumer, _))) -> consumer.SeekAsync(ts) :> Task)
                    |> Seq.toArray
                    |> Task.WhenAll
                    |> channel.Reply
                    return! loop ()

                | PatternTickTime ->
                    
                    Log.Logger.LogDebug("{0} PatternTickTime", prefix)
                    try 
                        match multiConsumerType with
                        | Pattern patternInfo ->
                            let! newAllTopics = patternInfo.GetTopics() |> Async.AwaitTask
                            let addedTopics = newAllTopics |> HashSet
                            let removedTopics = allTopics |> HashSet
                            addedTopics.ExceptWith allTopics
                            removedTopics.ExceptWith newAllTopics
                            if addedTopics.Count > 0 then
                                Log.Logger.LogInformation("{0} subscribing to {1} new topics", prefix, addedTopics.Count)
                                let! streams = processAddedTopics addedTopics patternInfo.GetConsumerInfo |> Async.AwaitTask
                                currentStream.AddGenerators(streams)
                            if removedTopics.Count > 0 then
                                Log.Logger.LogInformation("{0} removing subscription to {1} old topics", prefix, removedTopics.Count)
                                let! streams = processRemovedTopics removedTopics |> Async.AwaitTask
                                streams   
                                |> Seq.iter (fun stream -> currentStream.RemoveGenerator stream)
                            return! loop ()
                        | _ ->
                            Log.Logger.LogWarning("{0} PatternTickTime is not expected to be called for other multitopics types.", prefix)
                            return! loop ()
                    with ex ->
                        Log.Logger.LogWarning(ex, "{0} PatternTickTime failed.", prefix)
                        return! loop ()
                
                | PartitionTickTime  ->

                    
                    Log.Logger.LogDebug("{0} PartitionTickTime", prefix)
                    match this.ConnectionState with
                    | Ready ->
                        // Check partitions changes of passed in topics, and add new topic partitions.
                        let! newPartitionsOption =
                            getNewPartitions() 
                            |> Async.AwaitTask
                        match newPartitionsOption with
                        | Some newPartitions ->
                            let oldConsumersCount = consumers.Count
                            let mutable totalConsumersCount = oldConsumersCount
                            let topicsToUpdate =
                                newPartitions
                                |> Seq.filter(fun (topic, partitionedTopicNames) ->
                                        let oldPartitionsCount = partitionedTopics.[topic].Metadata.Partitions
                                        let newPartitionsCount = partitionedTopicNames.Length
                                        if (oldPartitionsCount < newPartitionsCount) then
                                            Log.Logger.LogDebug("{0} partitions number. old: {1}, new: {2}, topic {3}",
                                                                prefix, oldPartitionsCount, newPartitionsCount, topic)
                                            totalConsumersCount <- totalConsumersCount + (newPartitionsCount - oldPartitionsCount)
                                            true
                                        elif (oldPartitionsCount > newPartitionsCount) then
                                            Log.Logger.LogError("{0} not support shrink topic partitions. old: {1}, new: {2}, topic: {3}",
                                                                prefix, oldPartitionsCount, newPartitionsCount, topic)
                                            false
                                        else
                                            false)
                                |> Seq.toArray
                            if topicsToUpdate.Length > 0 then
                                Log.Logger.LogInformation("{0} adding subscription to {1} new partitions", prefix, topicsToUpdate.Length)
                                let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / totalConsumersCount)
                                let newConsumerTasks =
                                    seq {
                                        for (topic, partitionedTopicNames) in topicsToUpdate do
                                            let consumerInitInfo = partitionedTopics.[topic]
                                            let newConsumerTasks =
                                                seq { consumerInitInfo.Metadata.Partitions..partitionedTopicNames.Length - 1 }
                                                |> Seq.map (fun partitionIndex ->
                                                    let partitionedTopic = partitionedTopicNames.[partitionIndex]
                                                    let partititonedConfig = { consumerConfig with
                                                                                ReceiverQueueSize = receiverQueueSize
                                                                                Topics = seq { partitionedTopic } |> Seq.cache }
                                                    task {
                                                        let! result =
                                                             ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic, connectionPool, partitionIndex, true,
                                                                                  None, lookup, true, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider, interceptors, fun _ -> ())
                                                        return (partitionedTopic, result)
                                                    })
                                            yield! newConsumerTasks
                                    }                            
                                try
                                    let! newConsumerResults =
                                        newConsumerTasks
                                        |> Task.WhenAll
                                        |> Async.AwaitTask
                                    let newStreams =
                                        newConsumerResults
                                        |> Seq.map (fun (topic, consumer) ->
                                            let stream = getStream topic.CompleteTopicName consumer
                                            consumers.Add(topic.CompleteTopicName, (consumer :> IConsumer<'T>, stream))
                                            stream)
                                    currentStream.AddGenerators(newStreams)
                                    Log.Logger.LogDebug("{0} success create consumers for extended partitions. old: {1}, new: {2}",
                                        prefix, oldConsumersCount, totalConsumersCount )
                                with Flatten ex ->
                                    Log.Logger.LogWarning(ex, "{0} fail create consumers for extended partitions. old: {1}, new: {2}",
                                        prefix, oldConsumersCount, totalConsumersCount )
                                    do! newConsumerTasks
                                        |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                                        |> Seq.map (fun t ->
                                           let (_, consumer) = t.Result
                                           (consumer :> IConsumer<'T>).DisposeAsync().AsTask())
                                        |> Task.WhenAll
                                        |> Async.AwaitTask
                                        |> Async.Ignore
                                    Log.Logger.LogInformation("{0} disposed partially created consumers", prefix)
                                    
                        | None ->
                            ()
                    | _ ->
                        ()
                    return! loop ()

                | GetStats channel ->
                    
                    Log.Logger.LogDebug("{0} GetStats", prefix)
                    let statsTask =
                        consumers
                        |> Seq.map (fun (KeyValue(_, (consumer, _))) -> consumer.GetStatsAsync())
                        |> Task.WhenAll
                    channel.Reply statsTask
                    return! loop ()
                    
                | ReconsumeLater (msg, deliverAt, channel) ->
                    
                    Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                    let (consumer, _) = consumers.[msg.MessageId.TopicName]
                    task {
                        do! consumer.ReconsumeLaterAsync(msg, deliverAt)
                        unAckedMessageTracker.Remove msg.MessageId |> ignore
                    } |> channel.Reply
                    return! loop ()
                    
                | ReconsumeLaterCumulative (msg, delayTime, channel) ->
                    
                    Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                    let (consumer, _) = consumers.[msg.MessageId.TopicName]
                    task {
                        do! consumer.ReconsumeLaterCumulativeAsync(msg, delayTime)
                        unAckedMessageTracker.RemoveMessagesTill msg.MessageId |> ignore
                    } |> channel.Reply
                    return! loop ()
                                
                | Close channel ->

                    Log.Logger.LogDebug("{0} Close", prefix)
                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply <| Ok()
                    | _ ->
                        this.ConnectionState <- Closing
                        let consumerTasks = consumers |> Seq.map(fun (KeyValue(_, (consumer, _))) -> consumer.DisposeAsync().AsTask())
                        try
                            let! _ = Task.WhenAll consumerTasks |> Async.AwaitTask
                            this.ConnectionState <- Closed
                            Log.Logger.LogInformation("{0} closed", prefix)
                            stopConsumer()
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} could not close", prefix)
                            this.ConnectionState <- Failed
                            channel.Reply <| Error ex
                            return! loop ()
                        

                | Unsubscribe channel ->

                    Log.Logger.LogDebug("{0} Unsubscribe", prefix)
                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply <| Ok()
                    | _ ->
                        this.ConnectionState <- Closing
                        let consumerTasks = consumers |> Seq.map(fun (KeyValue(_, (consumer, _))) -> consumer.UnsubscribeAsync())
                        try
                            let! _ = Task.WhenAll consumerTasks |> Async.AwaitTask
                            this.ConnectionState <- Closed
                            Log.Logger.LogInformation("{0} unsubscribed", prefix)
                            stopConsumer()
                            channel.Reply <| Ok()
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} could not unsubscribe", prefix)
                            this.ConnectionState <- Failed    
                            channel.Reply <| Error ex
                            return! loop ()
            }

        loop ()
    )

    do
        if consumerConfig.AutoUpdatePartitions
        then
            partitionsTimer.AutoReset <- true
            partitionsTimer.Elapsed.Add(fun _ -> mb.Post PartitionTickTime)
            partitionsTimer.Start()
    do
        match multiConsumerType with
        | Pattern _ ->
            patternTimer.AutoReset <- true
            patternTimer.Elapsed.Add(fun _ -> mb.Post PatternTickTime)
            patternTimer.Start()
        | _ -> ()

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    member private this.Mb with get(): MailboxProcessor<MultiTopicConsumerMessage<'T>> = mb

    member this.ConsumerId with get() = consumerId

    override this.Equals consumer =
        consumerId = (consumer :?> IConsumer<'T>).ConsumerId

    override this.GetHashCode () = int consumerId

    member private this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and set(value) = Volatile.Write(&connectionState, value)

    member private this.InitInternal() =
        task {
            mb.Post Init
            return! consumerCreatedTsc.Task
        }

    static member InitPartitioned(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfo: ConsumerInitInfo<'T>, lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.Partitioned consumerInitInfo, lookup,
                                                   interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }
        
    static member InitMultiTopic(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfos: ConsumerInitInfo<'T>[], lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.MultiTopic consumerInitInfos, lookup,
                                                   interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }
        
    static member InitPattern(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            patternInfo: PatternInfo<'T>, lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool,
                                                   MultiConsumerType.Pattern patternInfo,
                                                   lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }

    interface IConsumer<'T> with

        member this.ReceiveAsync(cancellationToken: CancellationToken) =
            task {
                match! mb.PostAndAsyncReply(fun channel -> Receive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    return reraize exn
            }
            
        member this.ReceiveAsync() =
            _this.ReceiveAsync(CancellationToken.None)
            
        member this.BatchReceiveAsync(cancellationToken: CancellationToken) =
            task {
                match! mb.PostAndAsyncReply(fun channel -> BatchReceive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    return reraize exn
            }

        member this.BatchReceiveAsync() =
            _this.BatchReceiveAsync(CancellationToken.None)
            
        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                let! t = mb.PostAndAsyncReply(fun channel -> Acknowledge(channel, msgId))
                return! t
            }
            
        member this.AcknowledgeAsync (msgs: Messages<'T>) =
            task {
                for msg in msgs do
                    let! t = mb.PostAndAsyncReply(fun channel -> Acknowledge(channel, msg.MessageId))
                    do! t
            }

        member this.AcknowledgeCumulativeAsync (msgId: MessageId) =
            task {
                let! result = mb.PostAndAsyncReply(fun channel -> AcknowledgeCumulative(channel, msgId))
                return! result
            }

        member this.RedeliverUnacknowledgedMessagesAsync () =
            task {
                let! result = mb.PostAndAsyncReply(RedeliverAllUnacknowledged)
                return! result
            }

        member this.SeekAsync (_: MessageId) =
            Task.FromException<unit>(exn "Seek operation not supported on multitopics consumer")

        member this.SeekAsync (timestamp: uint64) =
            task {
                let! result = mb.PostAndAsyncReply(fun channel -> Seek(channel, timestamp))
                return! result
            }
            
        member this.GetLastMessageIdAsync () =
            Task.FromException<MessageId>(exn "GetLastMessageId operation not supported on multitopics consumer")

        member this.UnsubscribeAsync() =
            task {
                let! result = mb.PostAndAsyncReply(Unsubscribe)
                match result with
                | Ok () -> ()
                | Error ex -> reraize ex
            }

        member this.HasReachedEndOfTopic with get() =
            mb.PostAndReply(HasReachedEndOfTheTopic)

        member this.NegativeAcknowledge msgId =
            task {
                let! result = mb.PostAndAsyncReply(fun channel -> NegativeAcknowledge(channel, msgId))
                return! result
            }
            
        member this.NegativeAcknowledge (msgs: Messages<'T>) =
            task {
                for msg in msgs do
                    let! t = mb.PostAndAsyncReply(fun channel -> NegativeAcknowledge(channel, msg.MessageId))
                    do! t
            }

        member this.ConsumerId = consumerId

        member this.Topic = "MultiTopicsConsumer-" + Generators.getRandomName()

        member this.Name = consumerConfig.ConsumerName

        member this.GetStatsAsync() =
            task {
                let! allStatsTask = mb.PostAndAsyncReply(GetStats)
                let! allStats = allStatsTask
                return allStats |> statsReduce
            }
            
        member this.ReconsumeLaterAsync (msg: Message<'T>, deliverAt: int64) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLater(msg, deliverAt, channel))
                return! result
            }
            
        member this.ReconsumeLaterCumulativeAsync (msg: Message<'T>, deliverAt: int64) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLaterCumulative(msg, deliverAt, channel))
                return! result
            }
        
        member this.ReconsumeLaterAsync (msgs: Messages<'T>, deliverAt: int64) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                for msg in msgs do
                    let! result = mb.PostAndAsyncReply(fun channel -> ReconsumeLater(msg, deliverAt, channel))
                    return! result
            }
        
    interface IAsyncDisposable with
        
        member this.DisposeAsync() =
            task {
                match this.ConnectionState with
                | Closing | Closed ->
                    return ()
                | _ ->
                    let! result = mb.PostAndAsyncReply(Close)
                    match result with
                    | Ok () -> ()
                    | Error ex -> reraize ex
            } |> ValueTask