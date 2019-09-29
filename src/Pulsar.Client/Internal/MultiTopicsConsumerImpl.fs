namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Generic
open System
open Pulsar.Client.Internal
open System.Runtime.CompilerServices
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open System.Threading
open System.Runtime.InteropServices
open System.IO
open ProtoBuf
open pulsar.proto
open System.Timers
open FSharp.Control

type MultiTopicConnectionState =
    | Uninitialized
    | Failed
    | Ready
    | Closing
    | Closed

type MultiTopicConsumerMessage =
    | Init
    | Receive of AsyncReplyChannel<Async<Message option>>
    | Acknowledge of AsyncReplyChannel<Task<unit>> * MessageId
    | AcknowledgeCumulative of AsyncReplyChannel<Task<unit>> * MessageId
    | RedeliverUnacknowledgedMessages of AsyncReplyChannel<Task>
    | Close of AsyncReplyChannel<Task<unit>>
    | Unsubscribe of AsyncReplyChannel<Task<unit>>
    | HasReachedEndOfTheTopic of AsyncReplyChannel<bool>
    | TickTime

type MultipleTopicsConsumerState = {
    Stream: AsyncSeq<Message>
    Enumerator: IAsyncEnumerator<Message>
}

type MultiTopicsConsumerImpl private (consumerConfig: ConsumerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                      numPartitions: int, lookup: BinaryLookupService, cleanup: ConsumerImpl -> unit) as this =

    let consumerId = Generators.getNextConsumerId()
    let prefix = sprintf "mt/consumer(%u, %s)" %consumerId consumerConfig.ConsumerName
    let consumers = Dictionary<CompleteTopicName,IConsumer>(numPartitions)
    let consumerCreatedTsc = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let receiverQueueSize = Math.Min(consumerConfig.ReceiverQueueSize, consumerConfig.MaxTotalReceiverQueueSizeAcrossPartitions / numPartitions)
    let mutable connectionState = MultiTopicConnectionState.Uninitialized
    let mutable numPartitions = numPartitions
    let mutable active = true

    let timer = new Timer(1000.0 * 60.0) // 1 minute

    let getStream (topic: CompleteTopicName) (consumer: IConsumer) =
        asyncSeq {
            while not consumer.HasReachedEndOfTopic do
                let! message = (consumer.ReceiveAsync() |> Async.AwaitTask)
                let newMessageId = { message.MessageId with TopicName = topic }
                yield { message with MessageId = newMessageId }
        }

    

    let mb = MailboxProcessor<MultiTopicConsumerMessage>.Start(fun inbox ->

        let rec loop state =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Init ->
                    let consumerTasks =
                        Seq.init numPartitions (fun partitionIndex ->
                            let partitionedTopic = consumerConfig.Topic.GetPartition(partitionIndex)
                            let partititonedConfig = { consumerConfig with
                                                        ReceiverQueueSize = receiverQueueSize
                                                        Topic = partitionedTopic }
                            task {
                                let! result = ConsumerImpl.Init(partititonedConfig, clientConfig, connectionPool, partitionIndex, SubscriptionMode.Durable, lookup, fun _ -> ())
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
                                consumer.CloseAsync())
                            |> Task.WhenAll
                            |> Async.AwaitTask
                            |> Async.Ignore
                        this.ConnectionState <- Failed
                        consumerCreatedTsc.SetException(ex)
                        return! loop state

                | Receive channel ->

                    channel.Reply(state.Enumerator.MoveNext())
                    return! loop state

                | Acknowledge (channel, msgId) ->

                    let consumer = consumers.[msgId.TopicName]
                    channel.Reply(consumer.AcknowledgeAsync(msgId))
                    return! loop state

                | AcknowledgeCumulative (channel, msgId) ->

                    let consumer = consumers.[msgId.TopicName]
                    channel.Reply(consumer.AcknowledgeCumulativeAsync(msgId))
                    return! loop state

                | RedeliverUnacknowledgedMessages channel ->

                    match this.ConnectionState with
                    | Ready ->
                        consumers
                        |> Seq.map(fun consumer -> consumer.Value.RedeliverUnacknowledgedMessagesAsync())
                        |> Task.WhenAll
                        :> Task
                        |> channel.Reply
                    | _ ->
                        channel.Reply(Task.FromException(Exception(prefix + " invalid state: " + this.ConnectionState.ToString())))
                    return! loop state

                | Close channel ->

                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply(Task.FromResult())
                    | _ ->
                        this.ConnectionState <- Closing
                        timer.Close()
                        let consumerTasks = consumers |> Seq.map(fun kv -> kv.Value.CloseAsync())
                        task {
                            try
                                let! _ = Task.WhenAll consumerTasks
                                this.ConnectionState <- Closed
                                Log.Logger.LogInformation("{0} closed", prefix)
                            with ex ->
                                Log.Logger.LogError(ex, "{0} could not close", prefix)
                                this.ConnectionState <- Failed
                                return! loop state
                        } |> channel.Reply

                | Unsubscribe channel ->

                    match this.ConnectionState with
                    | Closing | Closed ->
                        channel.Reply(Task.FromResult())
                    | _ ->
                        this.ConnectionState <- Closing
                        timer.Close()
                        let consumerTasks = consumers |> Seq.map(fun kv -> kv.Value.UnsubscribeAsync())
                        task {
                            try
                                let! _ = Task.WhenAll consumerTasks
                                this.ConnectionState <- Closed
                                Log.Logger.LogInformation("{0} unsubscribed", prefix)
                            with ex ->
                                Log.Logger.LogError(ex, "{0} could not unsubscribe", prefix)
                                this.ConnectionState <- Failed
                                return! loop state
                        } |> channel.Reply

                | HasReachedEndOfTheTopic channel ->

                    consumers
                    |> Seq.forall (fun kv -> kv.Value.HasReachedEndOfTopic)
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
                                        let! result = ConsumerImpl.Init(partititonedConfig, clientConfig, connectionPool, partitionIndex, SubscriptionMode.Durable, lookup, fun _ -> ())
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
                                       consumer.CloseAsync())
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
                                            numPartitions: int, lookup: BinaryLookupService, cleanup: ConsumerImpl -> unit) =
        task {
            let producer = new MultiTopicsConsumerImpl(consumerConfig, clientConfig, connectionPool, numPartitions, lookup, cleanup)
            do! producer.InitInternal()
            return producer :> IConsumer
        }

    interface IConsumer with

        member this.ReceiveAsync() =
            task {
                let! t = mb.PostAndAsyncReply(Receive)
                match! t with
                | Some message ->
                    return message
                | None ->
                    return failwith "No messages available"
            }

        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                let! t = mb.PostAndAsyncReply(fun channel -> Acknowledge(channel, msgId))
                return! t
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



