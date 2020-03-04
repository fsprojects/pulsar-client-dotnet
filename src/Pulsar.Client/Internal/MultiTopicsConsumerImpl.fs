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
open FSharp.Control

type internal MultiTopicConnectionState =
    | Uninitialized
    | Failed
    | Ready
    | Closing
    | Closed

type internal BatchAddResponse =
    | Expired
    | BatchReady of Messages
    | Success

type internal MultiTopicConsumerMessage =
    | Init
    | Receive of AsyncReplyChannel<Async<ResultOrException<Message> option>>
    | BatchReceive of AsyncReplyChannel<Async<ResultOrException<Messages>>> * CancellationToken 
    | BatchReceiveCompleted
    | BatchTimeout of AsyncReplyChannel<ResultOrException<Messages>> * CancellationToken
    | AddBatchMessage of AsyncReplyChannel<BatchAddResponse> * ResultOrException<Message> * CancellationToken
    | Acknowledge of AsyncReplyChannel<Task<unit>> * MessageId
    | NegativeAcknowledge of AsyncReplyChannel<Task<unit>> * MessageId
    | AcknowledgeCumulative of AsyncReplyChannel<Task<unit>> * MessageId
    | RedeliverUnacknowledgedMessages of AsyncReplyChannel<Task>
    | Close of AsyncReplyChannel<Task<unit>>
    | Unsubscribe of AsyncReplyChannel<Task<unit>>
    | HasReachedEndOfTheTopic of AsyncReplyChannel<bool>
    | Seek of AsyncReplyChannel<Task> * uint64
    | TickTime



type internal MultipleTopicsConsumerState = {
    Stream: AsyncSeq<ResultOrException<Message>>
    Enumerator: IAsyncEnumerator<ResultOrException<Message>>
}

type internal MultiTopicsConsumerImpl private (consumerConfig: ConsumerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                      numPartitions: int, lookup: BinaryLookupService, interceptors: ConsumerInterceptors, cleanup: MultiTopicsConsumerImpl -> unit) as this =

    let consumerId = Generators.getNextConsumerId()
    let prefix = sprintf "mt/consumer(%u, %s)" %consumerId consumerConfig.ConsumerName
    let consumers = Dictionary<CompleteTopicName,IConsumer>(numPartitions)
    let consumerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / numPartitions)
    let mutable connectionState = MultiTopicConnectionState.Uninitialized
    let mutable numPartitions = numPartitions
    let mutable currentBatch = Messages(consumerConfig.BatchReceivePolicy.MaxNumMessages, consumerConfig.BatchReceivePolicy.MaxNumBytes)
    let batchWaiters = Queue<CancellationToken*AsyncReplyChannel<Async<ResultOrException<Messages>>>>()
    let mutable unhandleMessage: ResultOrException<Message> option = None
    
    let alreadyCancelledExn = Exn <| Exception "Batch already cancelled"
    let noMoreMessagesExn = Exn <| Exception "No more messages available"

    let timer = new Timer(1000.0 * 60.0) // 1 minute
    
    let getStream (topic: CompleteTopicName) (consumer: ConsumerImpl) =
        let consumerImp = consumer :> IConsumer
        asyncSeq {
            while not consumerImp.HasReachedEndOfTopic do
                let! message = consumer.ReceiveFsharpAsync()
                match message with
                | Result msg ->
                    let newMessageId = { msg.MessageId with TopicName = topic }
                    yield Result { msg with MessageId = newMessageId }
                | ex -> yield ex
        }
        
    let stopConsumer() =
        cleanup(this)
        timer.Close()
        while batchWaiters.Count > 0 do
            let _, batchWaitingChannel = batchWaiters.Dequeue()
            batchWaitingChannel.Reply(async{ return Exn (AlreadyClosedException("Consumer is already closed"))})

    let rec work ct =
        async {
            let! msgOption = this.Mb.PostAndAsyncReply(Receive)
            match! msgOption with
            | Some msg ->
                match! this.Mb.PostAndAsyncReply(fun ch -> AddBatchMessage(ch, msg, ct)) with
                | BatchReady msgs ->
                    Log.Logger.LogDebug("{0} completing batch work. {1} messages", prefix, msgs.Count)
                    return Result msgs
                | Success ->
                    Log.Logger.LogDebug("{0} adding one message to batch", prefix)
                    return! work ct
                | Expired ->
                    Log.Logger.LogDebug("{0} batch work expired", prefix)
                    return alreadyCancelledExn
            | _ ->
                Log.Logger.LogWarning("{0}: no messages available for Batch receive", prefix)
                return noMoreMessagesExn                                                
        }
        
    let completeBatch() =
        let batch = currentBatch
        currentBatch <- Messages(consumerConfig.BatchReceivePolicy.MaxNumMessages, consumerConfig.BatchReceivePolicy.MaxNumBytes)
        this.Mb.Post(BatchReceiveCompleted)
        batch
    
    let mb = MailboxProcessor<MultiTopicConsumerMessage>.Start(fun inbox ->

        let rec loop state =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Init ->
                    
                    Log.Logger.LogDebug("{0} Init", prefix)
                    let consumerTasks =
                        Seq.init numPartitions (fun partitionIndex ->
                            let partitionedTopic = consumerConfig.Topic.GetPartition(partitionIndex)
                            let partititonedConfig = { consumerConfig with
                                                        ReceiverQueueSize = receiverQueueSize
                                                        Topic = partitionedTopic }
                            task {
                                let! result =
                                    ConsumerImpl.Init(partititonedConfig, clientConfig, connectionPool, partitionIndex,
                                                      None, lookup, true, interceptors, fun _ -> ())
                                return (partitionedTopic, result)
                            })
                    try
                        let! consumerResults =
                            consumerTasks
                            |> Task.WhenAll
                            |> Async.AwaitTask
                        let newStream =
                            consumerResults
                                |> Seq.map (fun (topic, consumer) ->
                                    consumers.Add(topic.CompleteTopicName, consumer)
                                    getStream topic.CompleteTopicName consumer)
                                |> Seq.toList
                                |> AsyncSeq.mergeAll
                        this.ConnectionState <- Ready
                        Log.Logger.LogInformation("{0} created", prefix)
                        consumerCreatedTsc.SetResult()
                        return! loop { Stream = newStream; Enumerator = newStream.GetEnumerator()}
                    with ex ->
                        Log.Logger.LogError(ex, "{0} could not create", prefix)
                        do! consumerTasks
                            |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                            |> Seq.map (fun t ->
                                let (_, consumer) = t.Result
                                (consumer :> IConsumer).CloseAsync())
                            |> Task.WhenAll
                            |> Async.AwaitTask
                            |> Async.Ignore
                        this.ConnectionState <- Failed
                        consumerCreatedTsc.SetException(ex)
                        stopConsumer()
                        
                | Receive channel ->
                    
                    Log.Logger.LogDebug("{0} Receive", prefix)
                    match unhandleMessage with
                    | Some _ ->
                        let savedUnhandled = unhandleMessage
                        unhandleMessage <- None
                        channel.Reply(async { return savedUnhandled })
                    | None ->
                        channel.Reply(state.Enumerator.MoveNext())
                    return! loop state
                    
                | BatchReceive (channel, ct: CancellationToken) ->
                    
                    Log.Logger.LogDebug("{0} BatchReceive", prefix)
                    match unhandleMessage with
                    | Some (Exn exn) ->
                        unhandleMessage <- None
                        channel.Reply(async { return Exn exn })
                    | _ ->
                        if batchWaiters.Count = 0 then
                            channel.Reply(work ct)
                        else
                            batchWaiters.Enqueue(ct, channel)
                    
                    return! loop state

                | AddBatchMessage (channel, message, ct) ->
                    
                    Log.Logger.LogDebug("{0} AddBatchMessage", prefix)
                    if ct.IsCancellationRequested then
                        unhandleMessage <- Some message
                        channel.Reply(Expired)
                    else
                        match message with
                        | Result msg ->
                            if (currentBatch.CanAdd(msg)) then
                                currentBatch.Add(msg)
                                if currentBatch.IsFull then
                                    BatchReady <| completeBatch()
                                else
                                    Success
                            else
                                unhandleMessage <- Some message
                                BatchReady <| completeBatch()
                        | Exn _ ->
                            unhandleMessage <- Some message
                            BatchReady <| completeBatch()
                        |> channel.Reply
                        return! loop state
                
                | BatchReceiveCompleted ->
                    
                    Log.Logger.LogDebug("{0} BatchReceiveCompleted", prefix)
                    if batchWaiters.Count > 0 then
                        let ct, ch = batchWaiters.Dequeue()
                        ch.Reply(work ct)
                    return! loop state
                    
                | BatchTimeout (channel, ct) ->
                    
                    Log.Logger.LogDebug("{0} BatchTimeout", prefix)
                    if not ct.IsCancellationRequested then
                        completeBatch() |> Result |> channel.Reply
                    else
                        alreadyCancelledExn |> channel.Reply
                    return! loop state
                
                | Acknowledge (channel, msgId) ->

                    Log.Logger.LogDebug("{0} Acknowledge {1}", prefix, msgId)
                    let consumer = consumers.[msgId.TopicName]
                    channel.Reply(consumer.AcknowledgeAsync(msgId))
                    return! loop state

                | NegativeAcknowledge (channel, msgId) ->

                    Log.Logger.LogDebug("{0} NegativeAcknowledge {1}", prefix, msgId)
                    let consumer = consumers.[msgId.TopicName]
                    channel.Reply(consumer.NegativeAcknowledge(msgId))
                    return! loop state

                | AcknowledgeCumulative (channel, msgId) ->

                    Log.Logger.LogDebug("{0} AcknowledgeCumulative {1}", prefix, msgId)
                    let consumer = consumers.[msgId.TopicName]
                    channel.Reply(consumer.AcknowledgeCumulativeAsync(msgId))
                    return! loop state

                | RedeliverUnacknowledgedMessages channel ->

                    Log.Logger.LogDebug("{0} RedeliverUnacknowledgedMessages", prefix)
                    match this.ConnectionState with
                    | Ready ->
                        consumers
                        |> Seq.map(fun consumer -> consumer.Value.RedeliverUnacknowledgedMessagesAsync())
                        |> Seq.toArray
                        |> Task.WhenAll
                        |> channel.Reply
                    | _ ->
                        channel.Reply(Task.FromException(Exception(prefix + " invalid state: " + this.ConnectionState.ToString())))
                    return! loop state

                | Close channel ->

                    Log.Logger.LogDebug("{0} Close", prefix)
                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply(Task.FromResult())
                    | _ ->
                        this.ConnectionState <- Closing
                        let consumerTasks = consumers |> Seq.map(fun kv -> kv.Value.CloseAsync())
                        task {
                            try
                                let! _ = Task.WhenAll consumerTasks
                                this.ConnectionState <- Closed
                                Log.Logger.LogInformation("{0} closed", prefix)
                            with ex ->
                                Log.Logger.LogError(ex, "{0} could not close", prefix)
                                this.ConnectionState <- Failed
                        } |> channel.Reply
                        stopConsumer()

                | Unsubscribe channel ->

                    Log.Logger.LogDebug("{0} Unsubscribe", prefix)
                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply(Task.FromResult())
                    | _ ->
                        this.ConnectionState <- Closing
                        let consumerTasks = consumers |> Seq.map(fun kv -> kv.Value.UnsubscribeAsync())
                        task {
                            try
                                let! _ = Task.WhenAll consumerTasks
                                this.ConnectionState <- Closed
                                Log.Logger.LogInformation("{0} unsubscribed", prefix)
                            with ex ->
                                Log.Logger.LogError(ex, "{0} could not unsubscribe", prefix)
                                this.ConnectionState <- Failed        
                        } |> channel.Reply
                        stopConsumer()

                | HasReachedEndOfTheTopic channel ->

                    Log.Logger.LogDebug("{0} HasReachedEndOfTheTopic", prefix)
                    consumers
                    |> Seq.forall (fun kv -> kv.Value.HasReachedEndOfTopic)
                    |> channel.Reply
                    return! loop state

                | Seek (channel, ts) ->

                    Log.Logger.LogDebug("{0} Seek {1}", prefix, ts)
                    consumers
                    |> Seq.map (fun kv -> kv.Value.SeekAsync(ts) :> Task)
                    |> Seq.toArray
                    |> Task.WhenAll
                    |> channel.Reply
                    return! loop state

                | TickTime  ->

                    match this.ConnectionState with
                    | Ready ->
                        // Check partitions changes of passed in topics, and add new topic partitions.
                        let! partitionedTopicNames = lookup.GetPartitionsForTopic(consumerConfig.Topic) |> Async.AwaitTask
                        Log.Logger.LogDebug("{0} partitions number. old: {1}, new: {2}", prefix, numPartitions, partitionedTopicNames.Length )
                        if numPartitions = partitionedTopicNames.Length
                        then
                            // topic partition number not changed
                            ()
                        elif numPartitions < partitionedTopicNames.Length
                        then
                            let consumerTasks =
                                seq { numPartitions..partitionedTopicNames.Length - 1 }
                                |> Seq.map (fun partitionIndex ->
                                    let partitionedTopic = partitionedTopicNames.[partitionIndex]
                                    let partititonedConfig = { consumerConfig with
                                                                ReceiverQueueSize = receiverQueueSize
                                                                Topic = partitionedTopic }
                                    task {
                                        let! result =
                                            ConsumerImpl.Init(partititonedConfig, clientConfig, connectionPool, partitionIndex,
                                                              None, lookup, true, interceptors, fun _ -> ())
                                        return (partitionedTopic, result)
                                    })
                            try
                                let! consumerResults =
                                    consumerTasks
                                    |> Task.WhenAll
                                    |> Async.AwaitTask
                                let newStream =
                                    consumerResults
                                    |> Seq.map (fun (topic, consumer) ->
                                        consumers.Add(topic.CompleteTopicName, consumer)
                                        getStream topic.CompleteTopicName consumer)
                                    |> Seq.toList
                                    |> AsyncSeq.mergeAll
                                Log.Logger.LogDebug("{0} success create consumers for extended partitions. old: {1}, new: {2}",
                                    prefix, numPartitions, partitionedTopicNames.Length )
                                numPartitions <- partitionedTopicNames.Length
                                return! loop { Stream = newStream; Enumerator = newStream.GetEnumerator()}
                            with ex ->
                                do! consumerTasks
                                    |> Seq.filter (fun t -> t.Status = TaskStatus.RanToCompletion)
                                    |> Seq.map (fun t ->
                                       let (_, consumer) = t.Result
                                       (consumer :> IConsumer).CloseAsync())
                                    |> Task.WhenAll
                                    |> Async.AwaitTask
                                    |> Async.Ignore
                                Log.Logger.LogWarning(ex, "{0} fail create consumers for extended partitions. old: {1}, new: {2}",
                                    prefix, numPartitions, partitionedTopicNames.Length )
                                return! loop state
                        else
                            Log.Logger.LogError("{0} not support shrink topic partitions. old: {1}, new: {2}",
                                prefix, numPartitions, partitionedTopicNames.Length )
                            return! loop state
                    | _ ->
                        ()
                        return! loop state

            }

        loop { Stream = AsyncSeq.empty; Enumerator = AsyncSeq.empty.GetEnumerator() }
    )

    do
        if consumerConfig.AutoUpdatePartitions
        then
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> mb.Post TickTime)
            timer.Start()

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    member private this.Mb with get(): MailboxProcessor<MultiTopicConsumerMessage> = mb

    member this.ConsumerId with get() = consumerId

    override this.Equals consumer =
        consumerId = (consumer :?> ConsumerImpl).ConsumerId

    override this.GetHashCode () = int consumerId

    member private this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and set(value) = Volatile.Write(&connectionState, value)

    member private this.InitInternal() =
        task {
            mb.Post Init
            return! consumerCreatedTsc.Task
        }

    // create consumer for a single topic with already known partitions.
    // first create a consumer with no topic, then do subscription for already know partitionedTopic.
    static member Init(consumerConfig: ConsumerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                            numPartitions: int, lookup: BinaryLookupService, interceptors: ConsumerInterceptors, cleanup: MultiTopicsConsumerImpl -> unit) =
        task {
            let consumer = MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, numPartitions, lookup, interceptors, cleanup)
            do! consumer.InitInternal()
            return consumer
        }

    interface IConsumer with

        member this.ReceiveAsync() =
            task {
                let! t = mb.PostAndAsyncReply(Receive)
                match! t with
                | Some result ->
                    match result with
                    | Result msg ->
                        return msg
                    | Exn exn ->
                        return reraize exn
                | None ->
                    return failwith "No more messages available"
            }
            
        member this.BatchReceiveAsync() =
            task {
                use cts = new CancellationTokenSource()
                let ct = cts.Token
                let receiveBatchTask =
                    task {
                        let! batchWork = mb.PostAndAsyncReply(fun channel -> BatchReceive(channel, ct))
                        if not ct.IsCancellationRequested then
                            return! batchWork
                        else
                            return alreadyCancelledExn
                    }
                let timeoutTask =
                    task {
                        do! Task.Delay (consumerConfig.BatchReceivePolicy.Timeout.TotalMilliseconds |> int)
                        if not ct.IsCancellationRequested then
                            return! mb.PostAndAsyncReply(fun channel -> BatchTimeout(channel, ct))                        
                        else
                            return alreadyCancelledExn
                    }
                let! result = seq { receiveBatchTask; timeoutTask } |> Task.WhenAny
                cts.Cancel()
                match! result with
                | Result msgs ->
                    return msgs
                | Exn exn ->
                    return reraize exn
            }

        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                let! t = mb.PostAndAsyncReply(fun channel -> Acknowledge(channel, msgId))
                return! t
            }
            
        member this.AcknowledgeAsync (msgs: Messages) =
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
                let! result = mb.PostAndAsyncReply(RedeliverUnacknowledgedMessages)
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

        member this.CloseAsync() =
            task {
                let! result = mb.PostAndAsyncReply(Close)
                return! result
            }

        member this.UnsubscribeAsync() =
            task {
                let! result = mb.PostAndAsyncReply(Unsubscribe)
                return! result
            }

        member this.HasReachedEndOfTopic with get() =
            mb.PostAndReply(HasReachedEndOfTheTopic)

        member this.NegativeAcknowledge msgId =
            task {
                let! result = mb.PostAndAsyncReply(fun channel -> NegativeAcknowledge(channel, msgId))
                return! result
            }
            
        member this.NegativeAcknowledge (msgs: Messages) =
            task {
                for msg in msgs do
                    let! t = mb.PostAndAsyncReply(fun channel -> NegativeAcknowledge(channel, msg.MessageId))
                    do! t
            }

        member this.ConsumerId = consumerId

        member this.Topic = %consumerConfig.Topic.CompleteTopicName

        member this.Name = consumerConfig.ConsumerName
