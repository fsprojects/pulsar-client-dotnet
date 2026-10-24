namespace Pulsar.Client.Api


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

type internal PolledMessage<'T> = (struct (CancellationToken * ResultOrException<Message<'T>>))

type internal ChildConsumerInfo<'T> =
    {
        // the subscribed topic, as tracked in allTopics
        TopicName: TopicName
        // the partition for partitioned topics, otherwise the topic itself
        ChildTopicName: TopicName
        // takes the parent's consumer epoch, otherwise the parent discards the new child's messages as stale
        Create: ConsumerEpoch -> Task<ConsumerImpl<'T>>
    }

type internal ChildConsumer<'T> =
    {
        Info: ChildConsumerInfo<'T>
        Consumer: IConsumer<'T>
        Stream: TaskGenerator<PolledMessage<'T>>
    }

type internal MultiTopicConsumerMessage<'T> =
    | Init
    | PollerFailed of exn
    | Receive of ReceiveCallback<'T>
    | BatchReceive of ReceiveCallbacks<'T>
    | MessageReceived of PolledMessage<'T> * TaskCompletionSource<unit>
    | SendBatchByTimeout
    | Acknowledge of TaskCompletionSource<unit> * MessageId * Transaction option
    | NegativeAcknowledge of TaskCompletionSource<unit> * MessageId
    | AcknowledgeCumulative of TaskCompletionSource<unit> * MessageId * Transaction option
    | RedeliverUnacknowledged of RedeliverSet
    | RedeliverAllUnacknowledged of TaskCompletionSource<unit> option
    | Close of TaskCompletionSource<unit>
    | Unsubscribe of TaskCompletionSource<unit>
    | HasReachedEndOfTheTopic of TaskCompletionSource<bool>
    | Seek of SeekType * TaskCompletionSource<unit>
    | SeekWithResolver of Func<string, SeekType> *  TaskCompletionSource<unit>
    | PatternTickTime
    | PartitionTickTime
    | GetStats of TaskCompletionSource<ConsumerStats array>
    | ReconsumeLater of Message<'T> * TimeStamp * TaskCompletionSource<unit>
    | ReconsumeLaterCumulative of Message<'T> * TimeStamp * TaskCompletionSource<unit>
    | CancelWaiter of Waiter<'T>
    | CancelBatchWaiter of BatchWaiter<'T>
    | LastDisconnectedTimestamp of TaskCompletionSource<TimeStamp>
    | IsConnected of TaskCompletionSource<bool>
    | HasMessageAvailable of TaskCompletionSource<bool>

type internal MultiTopicsConsumerImpl<'T> (consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                      multiConsumerType: MultiConsumerType<'T>, startMessageId: MessageId option,
                                      startMessageRollbackDuration: TimeSpan, lookup: ILookupService,
                                      interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) as this =

    let _this = this :> IConsumer<'T>
    let consumerId = Generators.getNextConsumerId()
    let consumerName = getConsumerName consumerConfig.ConsumerName
    let prefix = $"mt/consumer({consumerId}, {consumerName})"
    let consumers = Dictionary<CompleteTopicName, ChildConsumer<'T>>()
    // children are closed while the parent keeps running, e.g. when discovery removes their topic,
    // so only the parent disposes the interceptors
    let childInterceptors = interceptors.Borrow()
    let consumerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let pollerCts = new CancellationTokenSource()
    let mutable receiveCts = new CancellationTokenSource()
    // Late poller completions can restart a generator after shutdown disposes the source.
    let mutable receiveToken = receiveCts.Token
    let mutable connectionState = MultiTopicConnectionState.Uninitialized
    // why the consumer failed, so requests made after it stopped can report it
    let mutable terminalError: exn = null
    let mutable currentStream = Unchecked.defaultof<TaskSeq<PolledMessage<'T>>>
    let partitionedTopics = Dictionary<TopicName, ConsumerInitInfo<'T>>()
    let allTopics = HashSet()
    let mutable incomingMessagesSize = 0L
    let mutable currentConsumerEpoch: ConsumerEpoch = %0UL
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
        post this.Mb (RedeliverUnacknowledged messages)

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

    // as in the Java client
    let childReceiveRetryDelay = TimeSpan.FromSeconds(10.0)

    // As in the Java client, a child never fails the parent: a closed child is no longer read, and any other
    // receive failure is retried later. Neither is buffered as a message.
    let getStream (topic: CompleteTopicName) (consumer: ConsumerImpl<'T>) =
        Log.Logger.LogDebug("{0} getStream", topic)
        let consumerImp = consumer :> IConsumer<'T>
        fun () ->
            let cancellationToken = receiveToken
            backgroundTask {
                try
                    let mutable result = None
                    while result.IsNone do
                        cancellationToken.ThrowIfCancellationRequested()
                        let! hasReachedEndOfTopic = consumerImp.HasReachedEndOfTopic()
                        if hasReachedEndOfTopic then
                            Log.Logger.LogWarning("{0} topic was terminated", topic)
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                        let! message =
                            try
                                consumer.ReceiveWrappedAsync(cancellationToken)
                            with ex ->
                                // a stopped child throws before it posts the receive
                                Task.FromResult(Error ex)
                        match message with
                        | Error _ when cancellationToken.IsCancellationRequested ->
                            result <- Some message
                        | Error (:? AlreadyClosedException) ->
                            // it stays registered until the parent closes or discovery removes its topic
                            Log.Logger.LogDebug("{0} stopped reading the closed consumer for {1}", prefix, topic)
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                        | Error ex ->
                            Log.Logger.LogError(ex, "{0} receive failed on the consumer for {1}, retrying in {2}",
                                                prefix, topic, childReceiveRetryDelay)
                            do! Task.Delay(childReceiveRetryDelay, cancellationToken)
                        | Ok msg ->
                            let newMessageId = { msg.MessageId with TopicName = topic }
                            result <- Some (Ok (msg.WithMessageId(newMessageId)))
                    return struct (cancellationToken, result.Value)
                with :? OperationCanceledException as ex when cancellationToken.IsCancellationRequested ->
                    return struct (cancellationToken, Error ex)
            }

    let resetStreams (action: IConsumer<'T> -> Task<unit>) =
        backgroundTask {
            let children = consumers.Values |> Seq.toArray
            for child in children do
                do! currentStream.RemoveGenerator child.Stream
            // Stop prefetch before moving any cursor, including reads whose results are already queued.
            receiveCts.Cancel()
            try
                let! _ =
                    children
                    |> Array.map (fun child -> backgroundTask { do! action child.Consumer })
                    |> Task.WhenAll
                ()
            finally
                receiveCts.Dispose()
                receiveCts <- new CancellationTokenSource()
                receiveToken <- receiveCts.Token
                currentStream.AddGenerators (children |> Array.map _.Stream)
        }

    let stopConsumer() =
        cleanup(this)
        partitionsTimer.Close()
        patternTimer.Close()
        unAckedMessageTracker.Close()
        pollerCts.Cancel()
        pollerCts.Dispose()
        receiveCts.Cancel()
        receiveCts.Dispose()
        interceptors.Close()
        let closedException = AlreadyClosedException("Consumer is already closed", terminalError)
        while waiters.Count > 0 do
            let waitingChannel = waiters |> dequeueWaiter
            closedException |> waitingChannel.TrySetException |> ignore
        while batchWaiters.Count > 0 do
            let batchWaitingChannel = batchWaiters |> dequeueBatchWaiter
            closedException |> batchWaitingChannel.TrySetException |> ignore
        Log.Logger.LogInformation("{0} stopped", prefix)

    let failConsumer (ex: exn) =
        terminalError <- ex
        this.ConnectionState <- Failed
        stopConsumer()

    let childConsumerInfo (consumerInitInfo: ConsumerInitInfo<'T>) (childTopic: TopicName) partitionIndex
                          receiverQueueSize createTopicIfDoesNotExist =
        let childConfig = { consumerConfig with
                                ReceiverQueueSize = receiverQueueSize
                                Topics = [| childTopic |] }
        {
            TopicName = consumerInitInfo.TopicName
            ChildTopicName = childTopic
            Create = fun consumerEpoch ->
                ConsumerImpl.Init(childConfig, clientConfig, childConfig.SingleTopic,
                                  connectionPool, partitionIndex, true, startMessageId, startMessageRollbackDuration,
                                  lookup, createTopicIfDoesNotExist, consumerInitInfo.Schema, consumerInitInfo.SchemaProvider,
                                  childInterceptors, (fun _ -> ()), consumerEpoch = consumerEpoch)
        }

    // only called from the mailbox, so the children are never changed concurrently
    let registerChild (info: ChildConsumerInfo<'T>) (consumer: ConsumerImpl<'T>) =
        let childTopic = info.ChildTopicName.CompleteTopicName
        let stream = getStream childTopic consumer
        consumers.Add(childTopic, { Info = info; Consumer = consumer :> IConsumer<'T>; Stream = stream })
        stream

    // the topic was removed
    let noChildException (topic: CompleteTopicName) =
        NotConnectedException($"{prefix} has no consumer for {topic}") :> exn

    let disposeCreatedChildren (consumersTasks: Task<ConsumerImpl<'T>> seq) =
        consumersTasks
        |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
        |> Seq.map (fun t -> (t.Result :> IConsumer<'T>).DisposeAsync().AsTask())
        |> Task.WhenAll

    let closeChildren () =
        consumers.Values
        |> Seq.map (fun child -> child.Consumer.DisposeAsync().AsTask())
        |> Task.WhenAll

    let singleInit (consumerInitInfo: ConsumerInitInfo<'T>) =
        let topic = consumerInitInfo.TopicName
        let numPartitions = consumerInitInfo.Metadata.Partitions
        let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / numPartitions)
        let infos =
            Array.init numPartitions (fun partitionIndex ->
                childConsumerInfo consumerInitInfo (topic.GetPartition partitionIndex) partitionIndex receiverQueueSize true)
        let consumersTasks = infos |> Array.map (fun info -> info.Create currentConsumerEpoch)
        backgroundTask {
            let! consumerResults = consumersTasks |> Task.WhenAll
            allTopics.Add topic |> ignore
            partitionedTopics.Add(topic, consumerInitInfo)
            return Array.map2 registerChild infos consumerResults
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
            let infos =
                consumerInitInfos
                |> Array.collect (fun consumerInitInfo ->
                    let topic = consumerInitInfo.TopicName
                    if consumerInitInfo.Metadata.IsMultiPartitioned then
                        Array.init consumerInitInfo.Metadata.Partitions (fun partitionIndex ->
                            childConsumerInfo consumerInitInfo (topic.GetPartition partitionIndex) partitionIndex
                                receiverQueueSize createTopicIfDoesNotExist)
                    else
                        [| childConsumerInfo consumerInitInfo topic (-1) receiverQueueSize createTopicIfDoesNotExist |])
            let consumersTasks = infos |> Array.map (fun info -> info.Create currentConsumerEpoch)
            backgroundTask {
                let! consumerResults = consumersTasks |> Task.WhenAll
                allTopics.UnionWith newTopics
                for consumerInfo in newPartitionedConsumers do
                    partitionedTopics.Add(consumerInfo.TopicName, consumerInfo)
                return Array.map2 registerChild infos consumerResults
            }, consumersTasks
        else
            Task.FromResult([||]), [||]

    let processAddedTopics (topicsToAdd: TopicName seq) (getConsumerInitInfo: TopicName -> Task<ConsumerInitInfo<'T>>) =
        backgroundTask {
            let! consumerInfos =
                topicsToAdd
                |> Seq.map getConsumerInitInfo
                |> Task.WhenAll
            let addedTopicsTask, consumersTasks = multiInit consumerInfos false
            try
                return! addedTopicsTask
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} could not processAddedTopics", prefix)
                do! disposeCreatedChildren consumersTasks
                return [||]
        }

    let processRemovedTopics (topicsToRemove: HashSet<TopicName>) =
        backgroundTask {
            let removedChildren =
                consumers
                |> Seq.filter (fun (KeyValue(_, child)) -> topicsToRemove.Contains child.Info.TopicName)
                |> Seq.toArray
            for KeyValue(childTopic, child) in removedChildren do
                do! currentStream.RemoveGenerator child.Stream
                consumers.Remove(childTopic) |> ignore
            for removedTopic in topicsToRemove do
                allTopics.Remove(removedTopic) |> ignore
                partitionedTopics.Remove(removedTopic) |> ignore
            try
                do! removedChildren
                    |> Array.map (fun (KeyValue(_, child)) -> child.Consumer.DisposeAsync().AsTask())
                    |> Task.WhenAll
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} could not processRemovedTopics fully", prefix)
        }

    let isPollingAllowed() =
        incomingMessages.Count <= sharedQueueResumeThreshold

    let isInvalidConsumerEpoch = isInvalidConsumerEpoch consumerConfig.SubscriptionType
    let isConsumerEpochSupported = isConsumerEpochSupported consumerConfig.SubscriptionType


    let enqueueMessage (m: ResultOrException<Message<'T>>) =
        match m with
        | Ok msg -> incomingMessagesSize <- incomingMessagesSize + msg.Data.LongLength
        | _ -> ()
        incomingMessages.Enqueue(m)
        
    let tryResumePoller() =
        if isPollingAllowed() && (waitingPoller <> defaultWaitingPoller) then
            Log.Logger.LogDebug("{0} resume poller", prefix)
            waitingPoller.SetResult()
            waitingPoller <- defaultWaitingPoller
    
    let clearIncomingMessages() =
        incomingMessages.Clear()
        incomingMessagesSize <- 0L
        tryResumePoller()

    let dequeueMessage() =
        let m = incomingMessages.Dequeue()
        match m with
        | Ok msg -> incomingMessagesSize <- incomingMessagesSize - msg.Data.LongLength
        | _ -> ()
        tryResumePoller()
        m

    let hasEnoughMessagesForBatchReceive() =
        hasEnoughMessagesForBatchReceive consumerConfig.BatchReceivePolicy incomingMessages.Count incomingMessagesSize

    let getAllPartitions () =
        backgroundTask {
            try
                let! results =
                    partitionedTopics
                    |> Seq.map (fun (KeyValue(topic, _)) -> backgroundTask {
                            let! partitionNames = lookup.GetPartitionsForTopic(topic)
                            return (topic, partitionNames)
                        })
                    |> Task.WhenAll
                return Some results
            with ex ->
               Log.Logger.LogWarning(ex, "{0} Unabled to fetch new topics", prefix)
               return None
        }

    let replyWithBatch (channel: TaskCompletionSource<Messages<'T>>) =
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
                        unAckedMessageTracker.Add msg.MessageId
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
            channel.SetException ex
        | _ ->
            Log.Logger.LogDebug("{0} BatchFormed with size {1}", prefix, messages.Size)
            channel.SetResult messages

    let handlePartitions() =
        backgroundTask {
            let! newPartitionsOption = getAllPartitions()
            match newPartitionsOption with
            | Some newPartitions ->
                let oldConsumersCount = consumers.Count
                let topicsToUpdate =
                    newPartitions
                    |> Array.filter(fun (topic, partitionedTopicNames) ->
                        let consumerInitInfo = partitionedTopics[topic]
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
                    // counted with the new partitions, so it is never zero
                    let totalConsumersCount =
                        topicsToUpdate
                        |> Array.sumBy (fun (topic, partitionedTopicNames) ->
                            partitionedTopicNames.Length - partitionedTopics[topic].Metadata.Partitions)
                        |> (+) oldConsumersCount
                    let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / totalConsumersCount)
                    let infos =
                        [|
                            for topic, partitionedTopicNames in topicsToUpdate do
                                let consumerInitInfo = partitionedTopics[topic]
                                let oldPartitionsCount = consumerInitInfo.Metadata.Partitions
                                let newPartitionsCount = partitionedTopicNames.Length
                                for partitionIndex in oldPartitionsCount..newPartitionsCount - 1 do
                                    let partitionedTopic = partitionedTopicNames[partitionIndex]
                                    childConsumerInfo consumerInitInfo partitionedTopic partitionIndex receiverQueueSize true
                        |]
                    let newConsumerTasks = infos |> Array.map (fun info -> info.Create currentConsumerEpoch)
                    try
                        let! newConsumers = newConsumerTasks |> Task.WhenAll
                        // the new partitions are only recorded once all of them have consumers, so a failed attempt is retried
                        for topic, partitionedTopicNames in topicsToUpdate do
                            partitionedTopics[topic] <-
                                {
                                    partitionedTopics[topic] with
                                        Metadata = {
                                            Partitions = partitionedTopicNames.Length
                                        }
                                }
                        let newStreams = Array.map2 registerChild infos newConsumers
                        currentStream.AddGenerators(newStreams)
                        Log.Logger.LogDebug("{0} success create consumers for extended partitions. old: {1}, new: {2}",
                            prefix, oldConsumersCount, totalConsumersCount )
                    with Flatten ex ->
                        Log.Logger.LogWarning(ex, "{0} fail create consumers for extended partitions. old: {1}, new: {2}",
                            prefix, oldConsumersCount, totalConsumersCount )
                        do! disposeCreatedChildren newConsumerTasks
                        Log.Logger.LogInformation("{0} disposed partially created consumers", prefix)

            | None ->
                ()
        }

    let replyWithMessage (channel: TaskCompletionSource<Message<'T>>) (message: ResultOrException<Message<'T>>) =
        match message with
        | Ok msg ->
            unAckedMessageTracker.Add msg.MessageId
            channel.SetResult msg
        | Error ex ->
            channel.SetException ex

    let receive (receiveCallback: ReceiveCallback<'T>) =
        Log.Logger.LogDebug("{0} Receive", prefix)
        let cancellationToken = receiveCallback.CancellationToken
        let channel = receiveCallback.MessageChannel
        if cancellationToken.IsCancellationRequested then
            channel.SetCanceled()
        else
            if incomingMessages.Count > 0 then
                replyWithMessage channel <| dequeueMessage()
            else
                let mutable synchronouslyCanceled = false
                let tokenRegistration =
                    if cancellationToken.CanBeCanceled then
                        let mutable cancellationTokenRegistration = None
                        cancellationTokenRegistration <-
                            cancellationToken.Register((fun () ->
                                Log.Logger.LogDebug("{0} receive cancelled", prefix)
                                match cancellationTokenRegistration with
                                | None -> synchronouslyCanceled <- true
                                | Some _ ->  post this.Mb (CancelWaiter(cancellationTokenRegistration, channel))
                            ), false) |> Some
                        cancellationTokenRegistration
                    else
                        None
                if synchronouslyCanceled then
                    channel.SetCanceled()
                    tokenRegistration |> Option.iter _.Dispose()
                else
                    waiters.AddLast(struct(tokenRegistration, channel)) |> ignore
                    Log.Logger.LogDebug("{0} Receive waiting", prefix)

    let batchReceive (receiveCallbacks: ReceiveCallbacks<'T>) =
        Log.Logger.LogDebug("{0} BatchReceive", prefix)
        let cancellationToken = receiveCallbacks.CancellationToken
        let channel = receiveCallbacks.MessagesChannel
        if cancellationToken.IsCancellationRequested then
            channel.SetCanceled()
        else
            if batchWaiters.Count = 0 && hasEnoughMessagesForBatchReceive() then
                replyWithBatch channel
            else
                let batchCts = new CancellationTokenSource()
                let mutable synchronouslyCanceled = false
                let tokenRegistration =
                    if cancellationToken.CanBeCanceled then
                        let mutable cancellationTokenRegistration = None
                        cancellationTokenRegistration <-
                            cancellationToken.Register((fun () ->
                                Log.Logger.LogDebug("{0} batch receive cancelled", prefix)
                                match cancellationTokenRegistration with
                                | None -> synchronouslyCanceled <- true
                                | Some _ -> post this.Mb (CancelBatchWaiter(batchCts, cancellationTokenRegistration, channel))
                            ), false)
                            |> Some
                        cancellationTokenRegistration
                    else
                        None
                if synchronouslyCanceled then
                    channel.SetCanceled()
                    tokenRegistration |> Option.iter _.Dispose()
                    batchCts.Dispose()
                else
                    batchWaiters.AddLast(struct(batchCts, tokenRegistration, channel)) |> ignore
                    asyncDelay
                        consumerConfig.BatchReceivePolicy.Timeout
                        (fun () ->
                            if not batchCts.IsCancellationRequested then
                                post this.Mb SendBatchByTimeout
                            else
                                batchCts.Dispose())
                    Log.Logger.LogDebug("{0} BatchReceive waiting", prefix)

    let runPoller (ct: CancellationToken) =
        (Task.Run<unit>(fun () ->
                backgroundTask {
                    while not ct.IsCancellationRequested do
                        let! msg = currentStream.Next()
                        if not ct.IsCancellationRequested then
                            do! postAndAsyncReply this.Mb (fun channel -> MessageReceived (msg, channel))
                        ()
                }
        , ct) :> Task).ContinueWith(fun t ->
                if t.IsFaulted then
                    let (Flatten ex) = t.Exception
                    Log.Logger.LogCritical(ex, "{0} poller failure", prefix)
                    post this.Mb (PollerFailed ex)
                else
                    Log.Logger.LogInformation("{0} poller has stopped normally", prefix)
            )

    let mb = Channel.CreateUnbounded<MultiTopicConsumerMessage<'T>>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))

    let replyFromStoppedMailbox (ex: exn) (msg: MultiTopicConsumerMessage<'T>) =
        match msg with
        | Receive receiveCallback ->
            receiveCallback.MessageChannel.TrySetException ex |> ignore
        | BatchReceive receiveCallbacks ->
            receiveCallbacks.MessagesChannel.TrySetException ex |> ignore
        | MessageReceived (_, pollerChannel) ->
            pollerChannel.TrySetException ex |> ignore
        | Acknowledge (channel, _, _)
        | NegativeAcknowledge (channel, _)
        | AcknowledgeCumulative (channel, _, _)
        | Unsubscribe channel
        | Seek (_, channel)
        | SeekWithResolver (_, channel)
        | ReconsumeLater (_, _, channel)
        | ReconsumeLaterCumulative (_, _, channel) ->
            channel.TrySetException ex |> ignore
        | RedeliverAllUnacknowledged (Some channel) ->
            channel.TrySetException ex |> ignore
        | HasReachedEndOfTheTopic channel
        | HasMessageAvailable channel ->
            channel.TrySetException ex |> ignore
        | GetStats channel ->
            channel.TrySetException ex |> ignore
        | LastDisconnectedTimestamp channel ->
            channel.TrySetException ex |> ignore
        | IsConnected channel ->
            channel.TrySetResult false |> ignore
        | CancelWaiter (struct (_, channel)) ->
            channel.TrySetCanceled() |> ignore
        | CancelBatchWaiter (struct (_, _, channel)) ->
            channel.TrySetCanceled() |> ignore
        | Close channel ->
            // the child consumers outlive a failed mailbox, so they still have to be closed
            backgroundTask {
                try
                    do! closeChildren()
                with Flatten closeEx ->
                    Log.Logger.LogError(closeEx, "{0} could not close all child consumers properly", prefix)
                this.ConnectionState <- Closed
                channel.TrySetResult() |> ignore
            } |> ignore
        | _ ->
            ()

    let stoppedMailboxException () =
        match this.ConnectionState with
        | Closing | Closed -> AlreadyClosedException(prefix + " already closed") :> exn
        | _ ->
            match terminalError with
            | null -> NotConnectedException(prefix + " not connected") :> exn
            | cause -> NotConnectedException($"{prefix} has failed and can't recover, it has to be recreated: {cause.Message}", cause) :> exn

    // requests posted after the mailbox exits would otherwise never be completed
    let drainStoppedMailbox () =
        backgroundTask {
            while true do
                let! msg = mb.Reader.ReadAsync()
                try
                    replyFromStoppedMailbox (stoppedMailboxException ()) msg
                with ex ->
                    Log.Logger.LogWarning(ex, "{0} failed to reply to a request after the mailbox stopped", prefix)
        } |> ignore

    let mutable currentMessage : MultiTopicConsumerMessage<'T> option = None

    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            currentMessage <- Some msg
            match msg with
            | Init ->

                Log.Logger.LogDebug("{0} Init", prefix)
                let newStreamsTask, consumersTasks =
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
                    this.ConnectionState <- Ready
                    Log.Logger.LogInformation("{0} created", prefix)
                    currentStream <- streams |> TaskSeq
                    runPoller pollerCts.Token |> ignore
                    consumerCreatedTsc.SetResult()
                with Flatten ex ->
                    Log.Logger.LogError(ex, "{0} could not create", prefix)
                    do! disposeCreatedChildren consumersTasks
                    failConsumer ex
                    consumerCreatedTsc.SetException(ex)

                if this.ConnectionState = Failed then
                    continueLoop <- false

            | PollerFailed ex ->
                Log.Logger.LogError(ex, "{0} stopping because its poller failed", prefix)
                failConsumer ex
                continueLoop <- false

            | MessageReceived (struct (cancellationToken, message), pollerChannel) ->

                let hasWaitingChannel = waiters.Count > 0
                let hasWaitingBatchChannel = batchWaiters.Count > 0
                Log.Logger.LogDebug("{0} MessageReceived queueLength={1}, hasWaitingChannel={2},  hasWaitingBatchChannel={3}",
                    prefix, incomingMessages.Count, hasWaitingChannel, hasWaitingBatchChannel)
                // handle message
                match message with
                | _ when cancellationToken.IsCancellationRequested ->
                    Log.Logger.LogDebug("{0} discarded a message from before the cursor reset", prefix)
                | Ok rawMessage when isInvalidConsumerEpoch rawMessage.ConsumerEpoch currentConsumerEpoch ->
                    Log.Logger.LogWarning("{0} Consumer filter old epoch message {1}, messageConsumerEpoch={2}, consumerEpoch={3}",
                        prefix, rawMessage.MessageId, rawMessage.ConsumerEpoch, currentConsumerEpoch)
                | _ ->
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
                if isPollingAllowed() then
                    pollerChannel.SetResult()
                else
                    Log.Logger.LogDebug("{0} paused poller, incomingMessages={1}, sharedQueueResumeThreshold={2}", prefix, incomingMessages.Count, sharedQueueResumeThreshold)
                    waitingPoller <- pollerChannel

            | Receive receiveCallback ->

                receive receiveCallback

            | BatchReceive receiveCallbacks ->

                batchReceive receiveCallbacks

            | SendBatchByTimeout ->

                Log.Logger.LogDebug("{0} SendBatchByTimeout", prefix)
                if batchWaiters.Count > 0 then
                    let ch = batchWaiters |> dequeueBatchWaiter
                    replyWithBatch ch

            | Acknowledge (channel, msgId, txnOption) ->

                Log.Logger.LogDebug("{0} Acknowledge {1}", prefix, msgId)
                match consumers.TryGetValue msgId.TopicName with
                | true, child ->
                    backgroundTask {
                        try
                            match txnOption with
                            | Some txn ->
                                do! child.Consumer.AcknowledgeAsync(msgId, txn)
                            | None ->
                                do! child.Consumer.AcknowledgeAsync(msgId)
                            unAckedMessageTracker.Remove msgId
                            channel.SetResult()
                        with Flatten ex ->
                            channel.SetException(ex)
                    } |> ignore
                | _ ->
                    channel.SetException(noChildException msgId.TopicName)

            | NegativeAcknowledge (channel, msgId) ->

                Log.Logger.LogDebug("{0} NegativeAcknowledge {1}", prefix, msgId)
                match consumers.TryGetValue msgId.TopicName with
                | true, child ->
                    backgroundTask {
                        try
                            do! child.Consumer.NegativeAcknowledge msgId
                            unAckedMessageTracker.Remove msgId
                            channel.SetResult()
                        with Flatten ex ->
                            channel.SetException ex
                    } |> ignore
                | _ ->
                    channel.SetException(noChildException msgId.TopicName)

            | AcknowledgeCumulative (channel, msgId, txnOption) ->

                Log.Logger.LogDebug("{0} AcknowledgeCumulative {1}", prefix, msgId)
                match consumers.TryGetValue msgId.TopicName with
                | true, child ->
                    backgroundTask {
                        try
                            match txnOption with
                            | Some txn ->
                                do! child.Consumer.AcknowledgeCumulativeAsync(msgId, txn)
                            | None ->
                                do! child.Consumer.AcknowledgeCumulativeAsync msgId
                            unAckedMessageTracker.RemoveMessagesTill msgId |> ignore
                            channel.SetResult()
                        with Flatten ex ->
                            channel.SetException ex
                    } |> ignore
                | _ ->
                    channel.SetException(noChildException msgId.TopicName)

            | RedeliverAllUnacknowledged channel ->

                Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                match this.ConnectionState with
                | Ready ->
                    try
                        clearIncomingMessages()
                        unAckedMessageTracker.Clear()
                        if isConsumerEpochSupported then
                            currentConsumerEpoch <- currentConsumerEpoch + %1UL
                        do! resetStreams (fun consumer ->
                            backgroundTask {
                                try
                                    do! consumer.RedeliverUnacknowledgedMessagesAsync()
                                with Flatten ex when (ex :? AlreadyClosedException) || (ex :? NotConnectedException) ->
                                    // as in the Java client, a stopped child has nothing to redeliver
                                    Log.Logger.LogDebug(ex, "{0} skipped redelivery for the stopped consumer for {1}", prefix, consumer.Topic)
                            })
                        channel |> Option.map _.SetResult() |> ignore
                    with ex ->
                        Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                        channel |> Option.map (fun ch -> ch.SetException ex) |> ignore
                | _ ->
                    Log.Logger.LogWarning(prefix + " invalid state: " + this.ConnectionState.ToString())
                    channel |> Option.map (fun ch -> ch.SetException(
                        Exception(prefix + " invalid state: " + this.ConnectionState.ToString()))) |> ignore

            | RedeliverUnacknowledged messageIds ->

                Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                match consumerConfig.SubscriptionType with
                | SubscriptionType.Shared | SubscriptionType.KeyShared ->
                    match this.ConnectionState with
                    | Ready ->
                        try
                            messageIds
                                |> Seq.groupBy _.TopicName
                                |> Seq.iter(fun (topicName, msgIds) ->
                                    match consumers.TryGetValue topicName with
                                    | true, child ->
                                        msgIds |> RedeliverSet |> (child.Consumer :?> ConsumerImpl<'T>).RedeliverUnacknowledged
                                    | _ ->
                                        // the topic was removed
                                        Log.Logger.LogDebug("{0} no consumer to redeliver messages for {1}", prefix, topicName)
                                    )
                        with ex ->
                            Log.Logger.LogError(ex, "{0} RedeliverUnacknowledgedMessages failed", prefix)
                    | _ ->
                        Log.Logger.LogWarning(prefix + " invalid state: " + this.ConnectionState.ToString())
                | _ ->
                    post this.Mb (RedeliverAllUnacknowledged None)
                    Log.Logger.LogInformation("{0} We cannot redeliver single messages if subscription type is not Shared", prefix)

            | CancelWaiter waiter ->

                if waiters.Remove waiter then
                    Log.Logger.LogDebug("{0} CancelWaiter, removed waiter", prefix)
                    let struct(ctrOpt, channel) = waiter
                    channel.SetCanceled()
                    ctrOpt |> Option.iter _.Dispose()
                else
                    Log.Logger.LogDebug("{0} CancelWaiter, no waiter found", prefix)

            | CancelBatchWaiter batchWaiter ->

                if batchWaiters.Remove batchWaiter then
                    Log.Logger.LogDebug("{0} CancelBatchWaiter, removed waiter", prefix)
                    let struct(batchCts, ctrOpt, channel) = batchWaiter
                    batchCts.Cancel()
                    batchCts.Dispose()
                    channel.SetCanceled()
                    ctrOpt |> Option.iter _.Dispose()
                else
                    Log.Logger.LogDebug("{0} CancelBatchWaiter, no waiter found", prefix)

            | HasReachedEndOfTheTopic channel ->

                Log.Logger.LogDebug("{0} HasReachedEndOfTheTopic", prefix)
                consumers
                |> Seq.forall (fun (KeyValue(_, child)) -> child.Consumer.HasReachedEndOfTopic().Result)
                |> channel.SetResult

            | LastDisconnectedTimestamp channel ->

                Log.Logger.LogDebug("{0} LastDisconnectedTimestamp", prefix)
                // there may be no children, e.g. when a pattern matches no topics
                consumers
                |> Seq.map (fun (KeyValue(_, child)) -> child.Consumer.LastDisconnectedTimestamp().Result)
                |> Seq.fold max %0L
                |> channel.SetResult

            | IsConnected channel ->

                Log.Logger.LogDebug("{0} IsConnected", prefix)
                consumers
                |> Seq.forall (fun (KeyValue(_, child)) -> child.Consumer.IsConnected().Result)
                |> channel.SetResult

            | Seek (seekData, channel) ->

                Log.Logger.LogDebug("{0} Seek {1}", prefix, seekData)
                try
                    unAckedMessageTracker.Clear()
                    clearIncomingMessages()
                    if isConsumerEpochSupported then
                        currentConsumerEpoch <- currentConsumerEpoch + %1UL
                    do! resetStreams (fun consumer ->
                        match seekData with
                        | SeekType.Timestamp ts -> consumer.SeekAsync(ts)
                        | SeekType.MessageId msgId -> consumer.SeekAsync(msgId))
                    channel.SetResult()
                with Flatten ex ->
                    channel.SetException ex

            | SeekWithResolver (resolver, channel) ->

                Log.Logger.LogDebug("{0} Seek with resolver", prefix)
                try
                    unAckedMessageTracker.Clear()
                    clearIncomingMessages()
                    if isConsumerEpochSupported then
                        currentConsumerEpoch <- currentConsumerEpoch + %1UL
                    do! resetStreams (fun consumer -> consumer.SeekAsync(resolver))
                    channel.SetResult()
                with Flatten ex ->
                    channel.SetException ex

            | PatternTickTime ->

                Log.Logger.LogDebug("{0} PatternTickTime", prefix)
                try
                    match multiConsumerType with
                    | Pattern patternInfo ->
                        let! newAllTopics = patternInfo.GetTopics()
                        let addedTopics = newAllTopics |> HashSet
                        let removedTopics = allTopics |> HashSet
                        addedTopics.ExceptWith allTopics
                        removedTopics.ExceptWith newAllTopics
                        if addedTopics.Count > 0 then
                            Log.Logger.LogInformation("{0} subscribing to {1} new topics", prefix, addedTopics.Count)
                            let! streams = processAddedTopics addedTopics patternInfo.GetConsumerInfo
                            currentStream.AddGenerators(streams)
                        if removedTopics.Count > 0 then
                            Log.Logger.LogInformation("{0} removing subscription to {1} old topics", prefix, removedTopics.Count)
                            do! processRemovedTopics removedTopics
                    | _ ->
                        Log.Logger.LogWarning("{0} PatternTickTime is not expected to be called for other multitopics types.", prefix)
                with ex ->
                    Log.Logger.LogWarning(ex, "{0} PatternTickTime failed.", prefix)

            | PartitionTickTime  ->

                Log.Logger.LogDebug("{0} PartitionTickTime", prefix)
                match this.ConnectionState with
                | Ready ->
                    try
                        // Check partitions changes of passed in topics, and add new topic partitions.
                        do! handlePartitions()
                    with ex ->
                        Log.Logger.LogWarning(ex, "{0} PartitionTickTime failed.", prefix)
                | _ ->
                    ()

            | GetStats channel ->

                Log.Logger.LogDebug("{0} GetStats", prefix)
                backgroundTask {
                    try
                        let! statsTask =
                            consumers
                            |> Seq.map (fun (KeyValue(_, child)) -> child.Consumer.GetStats())
                            |> Task.WhenAll
                        channel.SetResult(statsTask)
                    with Flatten ex ->
                        channel.SetException ex
                } |> ignore

            | ReconsumeLater (msg, deliverAt, channel) ->

                Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                match consumers.TryGetValue msg.MessageId.TopicName with
                | true, child ->
                    backgroundTask {
                        try
                            do! child.Consumer.ReconsumeLaterAsync(msg, deliverAt)
                            unAckedMessageTracker.Remove msg.MessageId
                            channel.SetResult()
                        with Flatten ex ->
                            channel.SetException ex
                    } |> ignore
                | _ ->
                    channel.SetException(noChildException msg.MessageId.TopicName)

            | ReconsumeLaterCumulative (msg, delayTime, channel) ->

                Log.Logger.LogDebug("{0} ReconsumeLater", prefix)
                match consumers.TryGetValue msg.MessageId.TopicName with
                | true, child ->
                    backgroundTask {
                        try
                            do! child.Consumer.ReconsumeLaterCumulativeAsync(msg, delayTime)
                            unAckedMessageTracker.RemoveMessagesTill msg.MessageId |> ignore
                            channel.SetResult()
                        with Flatten ex ->
                            channel.SetException ex
                    } |> ignore
                | _ ->
                    channel.SetException(noChildException msg.MessageId.TopicName)

            | HasMessageAvailable channel ->

                Log.Logger.LogDebug("{0} HasMessageAvailable", prefix)
                backgroundTask {
                    try
                        let! results =
                            consumers
                            |> Seq.map (fun (KeyValue(_, child)) -> (child.Consumer :?> ConsumerImpl<'T>).HasMessageAvailableAsync())
                            |> Task.WhenAll
                        results
                        |> Array.exists id
                        |> channel.SetResult
                    with Flatten ex ->
                        channel.SetException ex
                } |> ignore

            | Close channel ->

                Log.Logger.LogDebug("{0} Close", prefix)
                match this.ConnectionState with
                | Closing | Closed ->
                    channel.SetResult()
                | _ ->
                    this.ConnectionState <- Closing
                    try
                        do! closeChildren()
                        this.ConnectionState <- Closed
                        stopConsumer()
                        channel.SetResult()
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} could not close all child consumers properly", prefix)
                        this.ConnectionState <- Closed
                        stopConsumer()
                        channel.SetResult()
                continueLoop <- false

            | Unsubscribe channel ->

                Log.Logger.LogDebug("{0} Unsubscribe", prefix)
                continueLoop <- false
                match this.ConnectionState with
                | Closing | Closed ->
                    channel.SetResult()
                | _ ->
                    this.ConnectionState <- Closing
                    // as in the Java client, every child is tried even if a stopped one fails at once
                    let consumerTasks =
                        consumers
                        |> Seq.map (fun (KeyValue(_, child)) -> backgroundTask { do! child.Consumer.UnsubscribeAsync() })
                        |> Seq.toArray
                    try
                        let! _ = Task.WhenAll consumerTasks
                        this.ConnectionState <- Closed
                        Log.Logger.LogInformation("{0} unsubscribed", prefix)
                        stopConsumer()
                        channel.SetResult()
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "{0} could not unsubscribe", prefix)
                        this.ConnectionState <- Failed
                        channel.SetException ex
                        continueLoop <- true
        }:> Task).ContinueWith(fun t ->
            try
                if t.IsFaulted then
                    let (Flatten ex) = t.Exception
                    Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
                    terminalError <- ex
                    this.ConnectionState <- Failed
                    try
                        currentMessage |> Option.iter (replyFromStoppedMailbox ex)
                    with currentEx ->
                        Log.Logger.LogWarning(currentEx, "{0} failed to fail the in-flight request after mailbox failure", prefix)
                    stopConsumer()
                    consumerCreatedTsc.TrySetException(ex) |> ignore
                else
                    Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix)
            finally
                // only after the Failed transition and cleanup, so a queued Close can't race with them
                drainStoppedMailbox())
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


    member internal this.Mb with get(): Channel<MultiTopicConsumerMessage<'T>> = mb

    member internal this.Consumers =
        consumers.Values |> Seq.map _.Consumer |> Seq.toArray

    member this.ConsumerId with get() = consumerId

    override this.Equals consumer =
        consumerId = (consumer :?> IConsumer<'T>).ConsumerId

    override this.GetHashCode () = int consumerId

    member private this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and set value = Volatile.Write(&connectionState, value)

    member internal this.InitInternal() =
        backgroundTask {
            post mb Init
            return! consumerCreatedTsc.Task
        }

    member internal this.HasMessageAvailableAsync() =
        postAndAsyncReply mb HasMessageAvailable

    static member InitPartitioned(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfo: ConsumerInitInfo<'T>, lookup: ILookupService,
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        backgroundTask {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.Partitioned consumerInitInfo,
                                                   None, TimeSpan.Zero, lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }

    static member InitMultiTopic(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            consumerInitInfos: ConsumerInitInfo<'T>[], lookup: ILookupService,
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        backgroundTask {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, MultiConsumerType.MultiTopic consumerInitInfos,
                                                   None, TimeSpan.Zero, lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }

    static member InitPattern(consumerConfig: ConsumerConfiguration<'T>, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            patternInfo: PatternInfo<'T>, lookup: ILookupService,
                                            interceptors: ConsumerInterceptors<'T>, cleanup: MultiTopicsConsumerImpl<'T> -> unit) =
        backgroundTask {
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
            postAndAsyncReply mb (fun channel -> Receive { CancellationToken = cancellationToken; MessageChannel = channel })

        member this.ReceiveAsync() =
            _this.ReceiveAsync(CancellationToken.None)

        member this.BatchReceiveAsync(cancellationToken: CancellationToken) =
            postAndAsyncReply mb (fun channel -> BatchReceive { CancellationToken = cancellationToken; MessagesChannel = channel })

        member this.BatchReceiveAsync() =
            _this.BatchReceiveAsync(CancellationToken.None)

        member this.AcknowledgeAsync (msgId: MessageId) =
            postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, None))

        member this.AcknowledgeAsync (msgId: MessageId, txn: Transaction) =
            postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, Some txn))

        member this.AcknowledgeAsync (msgs: Messages<'T>) =
            backgroundTask {
                for msg in msgs do
                    do! postAndAsyncReply mb (fun channel -> Acknowledge(channel, msg.MessageId, None))
            }

        member this.AcknowledgeAsync (msgIds: MessageId seq) =
            backgroundTask {
                for msgId in msgIds do
                    do! postAndAsyncReply mb (fun channel -> Acknowledge(channel, msgId, None))
            }

        member this.AcknowledgeCumulativeAsync (msgId: MessageId) =
            postAndAsyncReply mb (fun channel -> AcknowledgeCumulative(channel, msgId, None))

        member this.AcknowledgeCumulativeAsync (msgId: MessageId, txn: Transaction) =
            postAndAsyncReply mb (fun channel -> AcknowledgeCumulative(channel, msgId, Some txn))

        member this.RedeliverUnacknowledgedMessagesAsync () =
            postAndAsyncReply mb (fun channel -> RedeliverAllUnacknowledged (Some channel))

        member this.SeekAsync (messageId: MessageId) =
            if MultiTopicsConsumerImpl<_>.isIllegalMultiTopicsMessageId messageId then
                failwith "Illegal messageId, messageId can only be earliest/latest"
            postAndAsyncReply mb (fun channel -> Seek(SeekType.MessageId messageId, channel))

        member this.SeekAsync (timestamp: TimeStamp) =
            postAndAsyncReply mb (fun channel -> Seek(SeekType.Timestamp timestamp, channel))

        member this.SeekAsync (resolver: Func<string, SeekType>) : Task<Unit>  =
            postAndAsyncReply mb (fun channel -> SeekWithResolver(resolver, channel))

        member this.GetLastMessageIdAsync () =
            Task.FromException<MessageId>(exn "GetLastMessageId operation not supported on multitopics consumer")

        member this.UnsubscribeAsync() =
            postAndAsyncReply mb Unsubscribe

        member this.HasReachedEndOfTopic() =
            postAndAsyncReply mb HasReachedEndOfTheTopic

        member this.NegativeAcknowledge msgId =
            postAndAsyncReply mb (fun channel -> NegativeAcknowledge(channel, msgId))

        member this.NegativeAcknowledge (msgs: Messages<'T>) =
            backgroundTask {
                for msg in msgs do
                    do! postAndAsyncReply mb (fun channel -> NegativeAcknowledge(channel, msg.MessageId))
            }

        member this.ConsumerId = consumerId

        member this.Topic = dummyTopicName

        member this.Name = consumerName

        member this.GetStats() =
            backgroundTask {
                let! allStats = postAndAsyncReply mb GetStats
                return allStats |> statsReduce
            }

        member this.ReconsumeLaterAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            if not consumerConfig.RetryEnable then
                failwith "Retry is disabled"
            postAndAsyncReply mb (fun channel -> ReconsumeLater(msg, deliverAt, channel))

        member this.ReconsumeLaterCumulativeAsync (msg: Message<'T>, deliverAt: TimeStamp) =
            if not consumerConfig.RetryEnable then
                failwith "Retry is disabled"
            postAndAsyncReply mb (fun channel -> ReconsumeLaterCumulative(msg, deliverAt, channel))

        member this.ReconsumeLaterAsync (msgs: Messages<'T>, deliverAt: TimeStamp) =
            backgroundTask {
                if not consumerConfig.RetryEnable then
                    failwith "Retry is disabled"
                for msg in msgs do
                    do! postAndAsyncReply mb (fun channel -> ReconsumeLater(msg, deliverAt, channel))
            }

        member this.LastDisconnectedTimestamp() =
            postAndAsyncReply mb LastDisconnectedTimestamp
        member this.IsConnected() =
            postAndAsyncReply mb IsConnected

    interface IAsyncDisposable with

        member this.DisposeAsync() =
            match this.ConnectionState with
            | Closing | Closed ->
                ValueTask()
            | _ ->
                postAndAsyncReply mb Close |> ValueTask