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
open ConsumerBase
open Pulsar.Client.Transaction
open System.Threading.Channels

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
    | Receive of CancellationToken * TaskCompletionSource<ResultOrException<Message<'T>>>
    | BatchReceive of CancellationToken * TaskCompletionSource<ResultOrException<Messages<'T>>>
    | MessageReceived of ResultOrException<Message<'T>> * TaskCompletionSource<unit>
    | SendBatchByTimeout
    | Acknowledge of TaskCompletionSource<Task<unit>> * MessageId * Transaction option
    | NegativeAcknowledge of TaskCompletionSource<Task<unit>> * MessageId
    | AcknowledgeCumulative of TaskCompletionSource<Task<unit>> * MessageId * Transaction option
    | RedeliverUnacknowledged of RedeliverSet * TaskCompletionSource<Task>
    | RedeliverAllUnacknowledged of TaskCompletionSource<Task>
    | Close of TaskCompletionSource<ResultOrException<unit>>
    | Unsubscribe of TaskCompletionSource<ResultOrException<unit>>
    | HasReachedEndOfTheTopic of TaskCompletionSource<bool>
    | Seek of SeekData * TaskCompletionSource<Task>
    | PatternTickTime
    | PartitionTickTime
    | GetStats of TaskCompletionSource<Task<ConsumerStats array>>
    | ReconsumeLater of Message<'T> * TimeStamp * TaskCompletionSource<Task<unit>>
    | ReconsumeLaterCumulative of Message<'T> * TimeStamp * TaskCompletionSource<Task<unit>>
    | RemoveWaiter of Waiter<'T>
    | RemoveBatchWaiter of BatchWaiter<'T>
    | LastDisconnectedTimestamp of TaskCompletionSource<TimeStamp>
    | HasMessageAvailable of TaskCompletionSource<Task<bool>>

type internal TopicAndConsumer<'T> =
    {
        IsPartitioned: bool
        TopicName: TopicName
        Consumer: ConsumerImpl<'T>
    }

type internal MultiTopicsConsumerImpl<'T> (consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                      multiConsumerType: MultiConsumerType<'T>, startMessageId: MessageId option,
                                      startMessageRollbackDuration: TimeSpan, lookup: BinaryLookupService,
                                      interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) as this =

    let _this = this :> IConsumer<'T>
    let consumerId = Generators.getNextConsumerId()
    let consumerName = getConsumerName consumerConfig.ConsumerName
    let prefix = $"mt/consumer({consumerId}, {consumerName})"
    let consumers = Dictionary<CompleteTopicName,IConsumer<'T> * TaskGenerator<ResultOrException<Message<'T>>>>()
    let consumerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let pollerCts = new CancellationTokenSource()
    let mutable connectionState = MultiTopicConnectionState.Uninitialized
    let mutable currentStream = Unchecked.defaultof<TaskSeq<ResultOrException<Message<'T>>>>
    let partitionedTopics = Dictionary<TopicName, ConsumerInitInfo<'T>>()
    let allTopics = HashSet()
    let mutable incomingMessagesSize = 0L
    let defaultWaitingPoller = Unchecked.defaultof<TaskCompletionSource<unit>>
    let mutable waitingPoller = defaultWaitingPoller
    let waiters = LinkedList<Waiter<'T>>()
    let batchWaiters = LinkedList<BatchWaiter<'T>>()
    let incomingMessages = Queue<ResultOrException<Message<'T>>>()
    let partitionsTimer = new Timer(consumerConfig.AutoUpdatePartitionsInterval.TotalMilliseconds)
    let patternTimer = new Timer(consumerConfig.PatternAutoDiscoveryPeriod.TotalMilliseconds)
    let sharedQueueResumeThreshold = consumerConfig.ReceiverQueueSize / 2
    let dummyTopicName = "MultiTopicsConsumer-" + Generators.getRandomName()
    
    let redeliverMessages messages =
        task {
            let! result = postAndAsyncReply this.Mb (fun channel -> RedeliverUnacknowledged (messages, channel))
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
            let waitingChannel = waiters |> dequeueWaiter
            waitingChannel.SetResult(Error (AlreadyClosedException("Consumer is already closed") :> exn))
        while batchWaiters.Count > 0 do
            let batchWaitingChannel = batchWaiters |> dequeueBatchWaiter
            batchWaitingChannel.SetResult(Error (AlreadyClosedException("Consumer is already closed") :> exn))
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
                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic,
                                          connectionPool, partitionIndex, true, startMessageId, startMessageRollbackDuration,
                                          lookup, true, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider,
                                          interceptors, fun _ -> ())
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
                                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic,
                                                          connectionPool, partitionIndex, true, startMessageId, startMessageRollbackDuration,
                                                          lookup, createTopicIfDoesNotExist, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider,
                                                          interceptors, fun _ -> ())
                                    return { IsPartitioned = true; TopicName = partitionedTopic; Consumer = result }
                                })
                        else
                            seq {
                                task {
                                    let partititonedConfig = { consumerConfig with
                                                                ReceiverQueueSize = receiverQueueSize
                                                                Topics = seq { consumerInitInfo.TopicName } |> Seq.cache }
                                    let! result =    
                                        ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic,
                                                          connectionPool, -1, true, startMessageId, startMessageRollbackDuration,
                                                          lookup, createTopicIfDoesNotExist, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider,
                                                          interceptors, fun _ -> ())
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
            waitingPoller.SetResult()
            waitingPoller <- defaultWaitingPoller
        m
    
    let hasEnoughMessagesForBatchReceive() =
        hasEnoughMessagesForBatchReceive consumerConfig.BatchReceivePolicy incomingMessages.Count incomingMessagesSize
        
    
    
    let getAllPartitions () =
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
        

    let replyWithBatch (ch: TaskCompletionSource<ResultOrException<Messages<'T>>>) =
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
            ch.SetResult (Error ex)
        | _ ->
            Log.Logger.LogDebug("{0} BatchFormed with size {1}", prefix, messages.Size)
            ch.SetResult (Ok messages)

    let handlePartitions() =
        task {
            let! newPartitionsOption = getAllPartitions() 
            match newPartitionsOption with
            | Some newPartitions ->
                let oldConsumersCount = consumers.Count
                let mutable totalConsumersCount = oldConsumersCount
                let topicsToUpdate =
                    newPartitions
                    |> Array.filter(fun (topic, partitionedTopicNames) ->
                        let consumerInitInfo = partitionedTopics.[topic]
                        let oldPartitionsCount = consumerInitInfo.Metadata.Partitions
                        let newPartitionsCount = partitionedTopicNames.Length
                        if (oldPartitionsCount < newPartitionsCount) then
                            Log.Logger.LogDebug("{0} partitions number. old: {1}, new: {2}, topic {3}",
                                                prefix, oldPartitionsCount, newPartitionsCount, topic)
                            true
                        elif (oldPartitionsCount > newPartitionsCount) then
                            Log.Logger.LogError("{0} not support shrink topic partitions. old: {1}, new: {2}, topic: {3}",
                                                prefix, oldPartitionsCount, newPartitionsCount, topic)
                            false
                        else
                            false)
                if topicsToUpdate.Length > 0 then
                    Log.Logger.LogInformation("{0} adding subscription to {1} new partitions", prefix, topicsToUpdate.Length)
                    let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / totalConsumersCount)
                    let newConsumerTasks =
                        seq {
                            for (topic, partitionedTopicNames) in topicsToUpdate do
                                let consumerInitInfo = partitionedTopics.[topic]
                                let oldPartitionsCount = consumerInitInfo.Metadata.Partitions
                                let newPartitionsCount = partitionedTopicNames.Length
                                totalConsumersCount <- totalConsumersCount + (newPartitionsCount - oldPartitionsCount)
                                partitionedTopics.[topic] <-
                                    {
                                        consumerInitInfo with
                                            Metadata = {
                                                consumerInitInfo.Metadata with Partitions = newPartitionsCount
                                            }
                                    }
                                let newConsumerTasks =
                                    seq { oldPartitionsCount..newPartitionsCount - 1 }
                                    |> Seq.map (fun partitionIndex ->
                                        let partitionedTopic = partitionedTopicNames.[partitionIndex]
                                        let partititonedConfig = { consumerConfig with
                                                                    ReceiverQueueSize = receiverQueueSize
                                                                    Topics = seq { partitionedTopic } |> Seq.cache }
                                        task {
                                            let! result =
                                                 ConsumerImpl.Init(partititonedConfig, clientConfig, partititonedConfig.SingleTopic,
                                                                   connectionPool, partitionIndex, true, startMessageId, startMessageRollbackDuration,
                                                                   lookup, true, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider,
                                                                   interceptors, fun _ -> ())
                                            return (partitionedTopic, result)
                                        })
                                yield! newConsumerTasks
                        }                            
                    try
                        let! newConsumerResults =
                            newConsumerTasks
                            |> Task.WhenAll
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
                        Log.Logger.LogInformation("{0} disposed partially created consumers", prefix)
                        
            | None ->
                ()
        }
    
    let replyWithMessage (channel: TaskCompletionSource<ResultOrException<Message<'T>>>) (message: ResultOrException<Message<'T>>) =
        match message with
        | Ok msg -> unAckedMessageTracker.Add msg.MessageId |> ignore
        | _ -> ()
        channel.SetResult message
    
    let runPoller (ct: CancellationToken) =
        Task.Run<unit>(fun () ->
                task {
                    while not ct.IsCancellationRequested do
                        let! msg = currentStream.Next()
                        if not ct.IsCancellationRequested then
                            do! postAndAsyncReply this.Mb (fun channel -> MessageReceived (msg, channel))
                        ()
                }
            , ct)
    
    let mb = Channel.CreateUnbounded<MultiTopicConsumerMessage<'T>>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (task {
        let mutable continueLoop = true
        while continueLoop do
            match! mb.Reader.ReadAsync() with
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
                
                if this.ConnectionState = Failed then
                    continueLoop <- false
                    
            | MessageReceived (message, pollerChannel) ->
                
                let hasWaitingChannel = waiters.Count > 0
                let hasWaitingBatchChannel = batchWaiters.Count > 0
                Log.Logger.LogDebug("{0} MessageReceived queueLength={1}, hasWaitingChannel={2},  hasWaitingBatchChannel={3}",
                    prefix, incomingMessages.Count, hasWaitingChannel, hasWaitingBatchChannel)
                // handle message
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
                // check if should reply to poller immediately
                if isPollingAllowed() |> not then
                    waitingPoller <- pollerChannel
                else
                    pollerChannel.SetResult()
                
            | Receive (cancellationToken, ch) ->
                
                Log.Logger.LogDebug("{0} Receive", prefix)
                if cancellationToken.IsCancellationRequested then
                    ch.SetResult(Error (TaskCanceledException() :> exn))
                else
                    if incomingMessages.Count > 0 then
                        replyWithMessage ch <| dequeueMessage()
                    else
                        let tokenRegistration =
                            if cancellationToken.CanBeCanceled then
                                let rec cancellationTokenRegistration =
                                    cancellationToken.Register((fun () ->
                                        Log.Logger.LogDebug("{0} receive cancelled", prefix)
                                        ch.SetResult(Error (TaskCanceledException() :> exn))
                                        post this.Mb (RemoveWaiter(cancellationTokenRegistration, ch))
                                    ), false) |> Some
                                cancellationTokenRegistration
                            else
                                None
                        waiters.AddLast((tokenRegistration, ch)) |> ignore
                        Log.Logger.LogDebug("{0} Receive waiting", prefix)
                
            | BatchReceive (cancellationToken, ch) ->
                
                Log.Logger.LogDebug("{0} BatchReceive", prefix)
                if cancellationToken.IsCancellationRequested then
                    ch.SetResult(Error (TaskCanceledException() :> exn))
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
                                        ch.SetResult(Error (TaskCanceledException() :> exn))
                                        post this.Mb (RemoveBatchWaiter(batchCts, cancellationTokenRegistration, ch))
                                    ), false)
                                    |> Some
                                cancellationTokenRegistration
                            else
                                None
                        batchWaiters.AddLast((batchCts, registration, ch)) |> ignore
                        asyncDelay
                            consumerConfig.BatchReceivePolicy.Timeout
                            (fun () ->
                                if not batchCts.IsCancellationRequested then
                                    post this.Mb SendBatchByTimeout
                                else
                                    batchCts.Dispose())
                        Log.Logger.LogDebug("{0} BatchReceive waiting", prefix)
                
            | SendBatchByTimeout ->
                
                Log.Logger.LogDebug("{0} SendBatchByTimeout", prefix)
                if batchWaiters.Count > 0 then
                    let ch = batchWaiters |> dequeueBatchWaiter
                    replyWithBatch ch
            
            | Acknowledge (channel, msgId, txnOption) ->

                Log.Logger.LogDebug("{0} Acknowledge {1}", prefix, msgId)
                let (consumer, _) = consumers.[msgId.TopicName]
                task {
                    match txnOption with
                    | Some txn ->
                        do! consumer.AcknowledgeAsync(msgId, txn)
                        unAckedMessageTracker.Remove msgId |> ignore
                    | None ->
                        do! consumer.AcknowledgeAsync(msgId)
                } |> channel.SetResult

            | NegativeAcknowledge (channel, msgId) ->

                Log.Logger.LogDebug("{0} NegativeAcknowledge {1}", prefix, msgId)
                let (consumer, _) = consumers.[msgId.TopicName]
                task {
                    do! consumer.NegativeAcknowledge msgId
                    unAckedMessageTracker.Remove msgId |> ignore
                } |> channel.SetResult

            | AcknowledgeCumulative (channel, msgId, txnOption) ->

                Log.Logger.LogDebug("{0} AcknowledgeCumulative {1}", prefix, msgId)
                let (consumer, _) = consumers.[msgId.TopicName]
                task {
                    match txnOption with
                    | Some txn ->
                        do! consumer.AcknowledgeCumulativeAsync(msgId, txn)
                    | None ->
                        do! consumer.AcknowledgeCumulativeAsync msgId
                    unAckedMessageTracker.RemoveMessagesTill msgId |> ignore
                } |> channel.SetResult
            
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
                        channel.SetResult(Task.FromResult())
                    with ex ->
                        Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                        channel.SetResult(Task.FromException ex)
                | _ ->
                    channel.SetResult(Task.FromException(Exception(prefix + " invalid state: " + this.ConnectionState.ToString())))
                
            | RedeliverUnacknowledged (messageIds, channel) ->

                Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                match consumerConfig.SubscriptionType with
                | SubscriptionType.Shared | SubscriptionType.KeyShared ->
                    match this.ConnectionState with
                    | Ready ->
                        try
                            do! messageIds
                                |> Seq.groupBy (fun msgId -> msgId.TopicName)
                                |> Seq.map(fun (topicName, msgIds) ->
                                    let (consumer, _) = consumers.[topicName]
                                    msgIds |> RedeliverSet |> (consumer :?> ConsumerImpl<'T>).RedeliverUnacknowledged
                                    )
                                |> Task.WhenAll :> Task
                            channel.SetResult(Task.FromResult())
                        with ex ->
                            Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                            channel.SetResult(Task.FromException ex)
                    | _ ->
                        channel.SetResult(Task.FromException <| Exception(prefix + " invalid state: " + this.ConnectionState.ToString()))
                | _ ->
                    post this.Mb (RedeliverAllUnacknowledged channel)
                    Log.Logger.LogInformation("{0} We cannot redeliver single messages if subscription type is not Shared", prefix)

            | RemoveWaiter waiter ->
                
                waiters.Remove(waiter) |> ignore
                let (ctrOpt, _) = waiter
                ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
                
            | RemoveBatchWaiter batchWaiter ->
                
                batchWaiters.Remove(batchWaiter) |> ignore
                let (cts, ctrOpt, _) = batchWaiter
                ctrOpt |> Option.iter (fun ctr -> ctr.Dispose())
                cts.Dispose()

            | HasReachedEndOfTheTopic channel ->

                Log.Logger.LogDebug("{0} HasReachedEndOfTheTopic", prefix)
                consumers
                |> Seq.forall (fun (KeyValue(_, (consumer, _))) -> consumer.HasReachedEndOfTopic)
                |> channel.SetResult
                
            | LastDisconnectedTimestamp channel ->
                
                Log.Logger.LogDebug("{0} LastDisconnectedTimestamp", prefix)
                consumers
                |> Seq.map (fun (KeyValue(_, (consumer, _))) -> consumer.LastDisconnectedTimestamp)
                |> Seq.max
                |> channel.SetResult

            | Seek (seekData, channel) ->

                Log.Logger.LogDebug("{0} Seek {1}", prefix, seekData)
                let seekTask = 
                    consumers
                    |> Seq.map (fun (KeyValue(_, (consumer, _))) ->
                        match seekData with
                        | Timestamp ts -> consumer.SeekAsync(ts)
                        | MessageId msgId -> consumer.SeekAsync(msgId))
                    |> Task.WhenAll
                unAckedMessageTracker.Clear()
                incomingMessages.Clear()
                incomingMessagesSize <- 0L
                channel.SetResult(seekTask)

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
                    | _ ->
                        Log.Logger.LogWarning("{0} PatternTickTime is not expected to be called for other multitopics types.", prefix)
                with ex ->
                    Log.Logger.LogWarning(ex, "{0} PatternTickTime failed.", prefix)
            
            | PartitionTickTime  ->
                
                Log.Logger.LogDebug("{0} PartitionTickTime", prefix)
                match this.ConnectionState with
                | Ready ->
                    // Check partitions changes of passed in topics, and add new topic partitions.
                    do! handlePartitions() |> Async.AwaitTask
                | _ ->
                    ()

            | GetStats channel ->
                
                Log.Logger.LogDebug("{0} GetStats", prefix)
                let statsTask =
                    consumers
                    |> Seq.map (fun (KeyValue(_, (consumer, _))) -> consumer.GetStatsAsync())
                    |> Task.WhenAll
                channel.SetResult(statsTask)
                
            | ReconsumeLater (msg, deliverAt, channel) ->
                
                Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                let (consumer, _) = consumers.[msg.MessageId.TopicName]
                task {
                    do! consumer.ReconsumeLaterAsync(msg, deliverAt)
                    unAckedMessageTracker.Remove msg.MessageId |> ignore
                } |> channel.SetResult
                
            | ReconsumeLaterCumulative (msg, delayTime, channel) ->
                
                Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                let (consumer, _) = consumers.[msg.MessageId.TopicName]
                task {
                    do! consumer.ReconsumeLaterCumulativeAsync(msg, delayTime)
                    unAckedMessageTracker.RemoveMessagesTill msg.MessageId |> ignore
                } |> channel.SetResult
                            
            | HasMessageAvailable channel ->
                
                Log.Logger.LogDebug("{0} HasMessageAvailable", prefix)
                task {
                    let! results =
                        consumers
                        |> Seq.map (fun (KeyValue(_, (consumer, _))) -> (consumer :?> ConsumerImpl<'T>).HasMessageAvailableAsync())
                        |> Task.WhenAll
                    return results
                    |> Array.exists id
                } |> channel.SetResult

                // Is this intentional?
                continueLoop <- false
                            
            | Close channel ->

                Log.Logger.LogDebug("{0} Close", prefix)
                match this.ConnectionState with
                | Closing | Closed ->
                    channel.SetResult(Ok())
                | _ ->
                    this.ConnectionState <- Closing
                    let consumerTasks = consumers |> Seq.map(fun (KeyValue(_, (consumer, _))) -> consumer.DisposeAsync().AsTask())
                    try
                        let! _ = Task.WhenAll consumerTasks |> Async.AwaitTask
                        this.ConnectionState <- Closed
                        stopConsumer()
                        channel.SetResult(Ok())
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} could not close all child consumers properly", prefix)
                        this.ConnectionState <- Closed
                        stopConsumer()
                        channel.SetResult(Ok())
                continueLoop <- false

            | Unsubscribe channel ->

                continueLoop <- false
                Log.Logger.LogDebug("{0} Unsubscribe", prefix)
                match this.ConnectionState with
                | Closing | Closed ->
                    channel.SetResult(Ok())
                | _ ->
                    this.ConnectionState <- Closing
                    let consumerTasks = consumers |> Seq.map(fun (KeyValue(_, (consumer, _))) -> consumer.UnsubscribeAsync())
                    try
                        let! _ = Task.WhenAll consumerTasks |> Async.AwaitTask
                        this.ConnectionState <- Closed
                        Log.Logger.LogInformation("{0} unsubscribed", prefix)
                        stopConsumer()
                        channel.SetResult(Ok())
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} could not unsubscribe", prefix)
                        this.ConnectionState <- Failed
                        channel.SetResult(Error ex)
                        continueLoop <- true
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then Log.Logger.LogCritical(t.Exception, "{0} mailbox failure", prefix)
            else Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    do
        if consumerConfig.AutoUpdatePartitions
        then
            partitionsTimer.AutoReset <- true
            partitionsTimer.Elapsed.Add(fun _ -> post mb PartitionTickTime)
            partitionsTimer.Start()
    do
        match multiConsumerType with
        | Pattern _ ->
            patternTimer.AutoReset <- true
            patternTimer.Elapsed.Add(fun _ -> post mb PatternTickTime)
            patternTimer.Start()
        | _ -> ()


    member private this.Mb with get(): Channel<MultiTopicConsumerMessage<'T>> = mb

    member this.ConsumerId with get() = consumerId

    override this.Equals consumer =
        consumerId = (consumer :?> IConsumer<'T>).ConsumerId

    override this.GetHashCode () = int consumerId

    member private this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and set(value) = Volatile.Write(&connectionState, value)

    member internal this.InitInternal() =
        task {
            post mb Init
            return! consumerCreatedTsc.Task
        }
        
    member internal this.HasMessageAvailableAsync() =
        task {
            let! result = postAndAsyncReply mb HasMessageAvailable
            return! result
        }

    static member InitPartitioned(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfo: ConsumerInitInfo<'T>, lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.Partitioned consumerInitInfo,
                                                   None, TimeSpan.Zero, lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }
        
    static member InitMultiTopic(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfos: ConsumerInitInfo<'T>[], lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.MultiTopic consumerInitInfos,
                                                   None, TimeSpan.Zero, lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }
        
    static member InitPattern(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            patternInfo: PatternInfo<'T>, lookup: BinaryLookupService, 
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool,
                                                   MultiConsumerType.Pattern patternInfo, None, TimeSpan.Zero,
                                                   lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }
        
    static member internal isIllegalMultiTopicsMessageId messageId =
        messageId <> MessageId.Earliest && messageId <> MessageId.Latest

    interface IConsumer<'T> with

        member this.ReceiveAsync(cancellationToken: CancellationToken) =
            task {
                match! postAndAsyncReply mb (fun channel -> Receive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    return reraize exn
            }
            
        member this.ReceiveAsync() =
            _this.ReceiveAsync(CancellationToken.None)
            
        member this.BatchReceiveAsync(cancellationToken: CancellationToken) =
            task {
                match! postAndAsyncReply mb (fun channel -> BatchReceive(cancellationToken, channel)) with
                | Ok msg ->
                    return msg
                | Error exn ->
                    return reraize exn
            }

        member this.BatchReceiveAsync() =
            _this.BatchReceiveAsync(CancellationToken.None)
            
        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                let! t = postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, None))
                return! t
            }
            
         member this.AcknowledgeAsync (msgId: MessageId, txn: Transaction) =
            task {
                let! t = postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, Some txn))
                return! t
            }
            
        member this.AcknowledgeAsync (msgs: Messages<'T>) =
            task {
                for msg in msgs do
                    let! t = postAndAsyncReply mb (fun channel -> Acknowledge(channel, msg.MessageId, None))
                    do! t
            }
            
        member this.AcknowledgeAsync (msgIds: MessageId seq) =
            task {
                for msgId in msgIds do
                    let! t = postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, None))
                    do! t
            }

        member this.AcknowledgeCumulativeAsync (msgId: MessageId) =
            task {
                let! result = postAndAsyncReply mb (fun channel -> AcknowledgeCumulative(channel, msgId, None))
                return! result
            }
            
        member this.AcknowledgeCumulativeAsync (msgId: MessageId, txn: Transaction) =
            task {
                let! result = postAndAsyncReply mb (fun channel -> AcknowledgeCumulative(channel, msgId, Some txn))
                return! result
            }

        member this.RedeliverUnacknowledgedMessagesAsync () =
            task {
                let! result = postAndAsyncReply mb RedeliverAllUnacknowledged
                return! result
            }

        member this.SeekAsync (messageId: MessageId) =
            if MultiTopicsConsumerImpl<_>.isIllegalMultiTopicsMessageId messageId then
                failwith "Illegal messageId, messageId can only be earliest/latest"
            task {
                let! result = postAndAsyncReply mb (fun channel -> Seek(MessageId messageId, channel))
                return! result
            }

        member this.SeekAsync (timestamp: TimeStamp) =
            task {
                let! result = postAndAsyncReply mb (fun channel -> Seek(Timestamp timestamp, channel))
                return! result
            }
            
        member this.GetLastMessageIdAsync () =
            Task.FromException<MessageId>(exn "GetLastMessageId operation not supported on multitopics consumer")

        member this.UnsubscribeAsync() =
            task {
                let! result = postAndAsyncReply mb Unsubscribe
                match result with
                | Ok () -> ()
                | Error ex -> reraize ex
            }

        member this.HasReachedEndOfTopic =
            postAndAsyncReply mb HasReachedEndOfTheTopic |> Async.AwaitTask |> Async.RunSynchronously

        member this.NegativeAcknowledge msgId =
            task {
                let! result = postAndAsyncReply mb (fun channel -> NegativeAcknowledge(channel, msgId))
                return! result
            }
            
        member this.NegativeAcknowledge (msgs: Messages<'T>) =
            task {
                for msg in msgs do
                    let! t = postAndAsyncReply mb (fun channel -> NegativeAcknowledge(channel, msg.MessageId))
                    do! t
            }

        member this.ConsumerId = consumerId

        member this.Topic = dummyTopicName

        member this.Name = consumerName

        member this.GetStatsAsync() =
            task {
                let! allStatsTask = postAndAsyncReply mb GetStats
                let! allStats = allStatsTask
                return allStats |> statsReduce
            }
            
        member this.ReconsumeLaterAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let! result = postAndAsyncReply mb (fun channel -> ReconsumeLater(msg, deliverAt, channel))
                return! result
            }
            
        member this.ReconsumeLaterCumulativeAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                let! result = postAndAsyncReply mb (fun channel -> ReconsumeLaterCumulative(msg, deliverAt, channel))
                return! result
            }
        
        member this.ReconsumeLaterAsync (msgs: Messages<'T>, deliverAt: TimeStamp) =
            task {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                for msg in msgs do
                    let! result = postAndAsyncReply mb (fun channel -> ReconsumeLater(msg, deliverAt, channel))
                    return! result
            }
            
        member this.LastDisconnectedTimestamp =
            postAndAsyncReply mb LastDisconnectedTimestamp |> Async.AwaitTask |> Async.RunSynchronously
        
    interface IAsyncDisposable with
        
        member this.DisposeAsync() =
            task {
                match this.ConnectionState with
                | Closing | Closed ->
                    return ()
                | _ ->
                    let! result = postAndAsyncReply mb Close
                    match result with
                    | Ok () -> ()
                    | Error ex -> reraize ex
            } |> ValueTask