module Pulsar.Client.IntegrationTests.Basic

open System
open System.Text.Json
open System.Threading
open System.Diagnostics
open System.Net.Http

open Expecto
open Expecto.Flip

open System.Collections.Concurrent
open System.Text
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

let assertMailboxException (work: Task<'T>) = task {
    try
        let! _ = work.WaitAsync(TimeSpan.FromSeconds(5.0))
        failwith "Task completed successfully after mailbox failure"
    with ex ->
        match unwrap ex with
        | :? NullReferenceException -> ()
        | :? TimeoutException -> failwith "Task did not fault within 5 seconds"
        | other -> failwith $"Expected the mailbox NullReferenceException but got {other.GetType().Name}: {other.Message}"
}

let assertAlreadyClosed (work: Task<'T>) = task {
    try
        let! _ = work.WaitAsync(TimeSpan.FromSeconds(5.0))
        failwith "Task completed successfully after the consumer was closed"
    with ex ->
        match unwrap ex with
        | :? AlreadyClosedException -> ()
        | :? TimeoutException -> failwith "Task did not fault within 5 seconds"
        | other -> failwith $"Expected AlreadyClosedException but got {other.GetType().Name}: {other.Message}"
}

// A null message faults a child consumer mailbox
let private crashChildMailbox (child: ConsumerImpl<byte[]>) =
    post child.Mb (ConsumerMessage.MessageReceived(struct (Unchecked.defaultof<RawMessage>, Unchecked.defaultof<ClientCnx>)))

// A null poll result faults the multi-topic consumer mailbox, and the poller reply is the in-flight request
let private crashMultiTopicMailbox (consumer: MultiTopicsConsumerImpl<byte[]>) (pollerReply: TaskCompletionSource<unit>) =
    post consumer.Mb
        (MultiTopicConsumerMessage.MessageReceived(struct (CancellationToken.None, Ok Unchecked.defaultof<Message<byte[]>>), pollerReply))

// a stopped consumer's mailbox only answers with an error
let private waitForStoppedChild (child: ConsumerImpl<byte[]>) =
    waitUntil "the child consumer has stopped" (fun () -> task {
        try
            let! _ = (child :> IConsumer<byte[]>).GetStats()
            return false
        with _ ->
            return true
    })

// A null reply channel faults the partitioned producer mailbox
let private crashPartitionedProducerMailbox (producer: PartitionedProducerImpl<byte[]>) =
    post producer.Mb (PartitionedProducerMessage.LastSequenceId Unchecked.defaultof<TaskCompletionSource<SequenceId>>)

let private createPartitionedTopic (topicName: string) (partitions: int) = task {
    use http = new HttpClient(new HttpClientHandler(UseProxy = false))
    http.BaseAddress <- Uri(pulsarHttpAddress)
    http.Timeout <- TimeSpan.FromSeconds(15.0)
    use content = new StringContent(string partitions, Encoding.UTF8, "application/json")
    let! created = http.PutAsync($"/admin/v2/persistent/{topicName}/partitions", content)
    created.EnsureSuccessStatusCode() |> ignore
}

type private CountingProducerInterceptor() =
    let mutable sent = 0
    let mutable acknowledged = 0
    let mutable disposed = 0
    member _.Sent = Volatile.Read(&sent)
    member _.Acknowledged = Volatile.Read(&acknowledged)
    member _.Disposed = Volatile.Read(&disposed)
    interface IProducerInterceptor<byte[]> with
        member _.Dispose() = Interlocked.Increment(&disposed) |> ignore
        member _.Eligible _ = true
        member _.BeforeSend(_, message) =
            Interlocked.Increment(&sent) |> ignore
            message
        member _.OnSendAcknowledgement(_, _, _, _) = Interlocked.Increment(&acknowledged) |> ignore

type private CountingInterceptor() =
    let mutable consumed = 0
    let mutable disposed = 0
    member _.Consumed = Volatile.Read(&consumed)
    member _.Disposed = Volatile.Read(&disposed)
    interface IConsumerInterceptor<byte[]> with
        member _.Dispose() = Interlocked.Increment(&disposed) |> ignore
        member _.BeforeConsume(_, message) =
            Interlocked.Increment(&consumed) |> ignore
            message
        member _.OnAcknowledge(_, _, _) = ()
        member _.OnAcknowledgeCumulative(_, _, _) = ()
        member _.OnNegativeAcksSend(_, _) = ()
        member _.OnAckTimeoutSend(_, _) = ()

// Runs `queue` inside the mailbox loop while it is still handling the request that completes `trigger`,
// so everything it posts is queued and read only after that request returns.
// `trigger` must not be created with RunContinuationsAsynchronously.
let queueBehind (trigger: TaskCompletionSource<'T>) (queue: unit -> unit) =
    trigger.Task.ContinueWith(Action<Task<'T>>(fun _ -> queue ()), TaskContinuationOptions.ExecuteSynchronously)
    |> ignore

// Takes `snapshot` on the thread that completes `request`, before that thread can do anything else.
// `request` must not be created with RunContinuationsAsynchronously.
let snapshotOnCompletion (request: TaskCompletionSource<'T>) (snapshot: unit -> 'S) =
    request.Task.ContinueWith(Func<Task<'T>, 'S>(fun _ -> snapshot ()), TaskContinuationOptions.ExecuteSynchronously)

// Swapping the process-wide logger has to be serialized. The observer forwards every
// message, and IsEnabled stays true so debug connection lines are still delivered to it.
let private loggerGate = new SemaphoreSlim(1, 1)

type private ObservingLogger(inner: Microsoft.Extensions.Logging.ILogger, onMessage: string -> unit) =
    interface Microsoft.Extensions.Logging.ILogger with
        member _.BeginScope<'TState>(state: 'TState) =
            inner.BeginScope(state)
        member _.IsEnabled(_) = true
        member _.Log<'TState>(logLevel, eventId, state, error, formatter) =
            if not (isNull (box formatter)) then
                let text = formatter.Invoke(state, error)
                if not (isNull text) then
                    onMessage text
            inner.Log<'TState>(logLevel, eventId, state, error, formatter)

let runWithLogger (onMessage: string -> unit) (action: unit -> Task<unit>) = task {
    do! loggerGate.WaitAsync()
    let previous = PulsarClient.Logger
    PulsarClient.Logger <- ObservingLogger(previous, onMessage)
    try
        do! action()
    finally
        PulsarClient.Logger <- previous
        loggerGate.Release() |> ignore
}

[<Tests>]
let tests =

    testList "Basic" [

        testTask "Consumer is closed at the broker when its mailbox fails" {

            Log.Debug("Started Consumer is closed at the broker when its mailbox fails")
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let observed = obj()
            let mutable consumerId = ""
            let mutable cnxPrefix = ""
            let mutable expectedCloseReply = ""
            let closeReply = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            do! runWithLogger (fun text ->
                lock observed (fun () ->
                    if consumerId = "" && text.Contains(topicName) && text.Contains("starting subscribe") then
                        let matched = System.Text.RegularExpressions.Regex.Match(text, @"consumer\((\d+),")
                        if matched.Success then
                            consumerId <- matched.Groups[1].Value
                    if cnxPrefix = "" && consumerId <> "" && text.EndsWith(" adding consumer " + consumerId) then
                        let marker = " adding consumer " + consumerId
                        cnxPrefix <- text.Substring(0, text.Length - marker.Length)
                    if cnxPrefix <> "" && text.StartsWith(cnxPrefix + " add request ") && text.EndsWith("type CloseConsumer") then
                        expectedCloseReply <- text.Replace(" add request ", " complete request ").Replace("type CloseConsumer", "type Success")
                    if expectedCloseReply <> "" && text = expectedCloseReply then
                        closeReply.TrySetResult() |> ignore)) (fun () -> task {
                    let client = getNewClient()
                    try
                        let! (consumer : IConsumer<byte[]>) =
                            client.NewConsumer()
                                .Topic(topicName)
                                .ConsumerName("failingMailbox")
                                .SubscriptionName("test-subscription")
                                .SubscriptionType(SubscriptionType.Exclusive)
                                .SubscribeAsync()

                        post (consumer :?> ConsumerImpl<byte[]>).Mb
                            (ConsumerMessage.MessageReceived(struct (Unchecked.defaultof<RawMessage>, Unchecked.defaultof<ClientCnx>)))
                        do! assertNotConnected (consumer.GetStats())
                        // A broker Success must complete a registered request, not be discarded as an unknown reply.
                        do! closeReply.Task.WaitAsync(TimeSpan.FromSeconds(5.0))

                        let! (consumer2 : IConsumer<byte[]>) =
                            client.NewConsumer()
                                .Topic(topicName)
                                .ConsumerName("replacement")
                                .SubscriptionName("test-subscription")
                                .SubscriptionType(SubscriptionType.Exclusive)
                                .SubscribeAsync()
                                .WaitAsync(TimeSpan.FromSeconds(15.0))
                        do! consumer2.UnsubscribeAsync()
                    finally
                        client.CloseAsync().GetAwaiter().GetResult()
                })

            Log.Debug("Finished Consumer is closed at the broker when its mailbox fails")
        }

        testTask "Requests posted after a consumer mailbox fails are completed" {

            Log.Debug("Started Requests posted after a consumer mailbox fails are completed")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingMailboxRequests")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            post (consumer :?> ConsumerImpl<byte[]>).Mb
                (ConsumerMessage.MessageReceived(struct (Unchecked.defaultof<RawMessage>, Unchecked.defaultof<ClientCnx>)))

            let deadline = DateTime.UtcNow.AddSeconds 5.0
            let mutable connected = true
            while connected do
                if DateTime.UtcNow >= deadline then
                    failwith "Consumer did not fail within 5 seconds"
                let! isConnected = consumer.IsConnected()
                if isConnected then
                    do! Task.Delay 100
                connected <- isConnected

            // GetStats still posts to the mailbox once the consumer has failed, so this only
            // returns if the stopped mailbox replies
            Expect.throwsT2<NotConnectedException> (fun () ->
                consumer.GetStats().WaitAsync(TimeSpan.FromSeconds(5.0)).Result |> ignore) |> ignore
            // Receive checks the connection before posting, and must fail rather than wait
            Expect.throwsT2<NotConnectedException> (fun () ->
                consumer.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0)).Result |> ignore) |> ignore
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Requests posted after a consumer mailbox fails are completed")
        }

        testTask "Queued messages release their payloads when the consumer mailbox fails" {

            Log.Debug("Started Queued messages release their payloads when the consumer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingMailboxPayload")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let payload = new System.IO.MemoryStream([| 1uy |])
            let queuedMessage =
                {
                    MessageId = MessageId.Earliest
                    ConsumerEpoch = Nullable()
                    Metadata = Unchecked.defaultof<_>
                    RedeliveryCount = 0
                    Payload = payload
                    MessageKey = null
                    IsKeyBase64Encoded = false
                    CheckSumValid = false
                    Properties = null
                    AckSet = null
                }
            let consumerImpl = consumer :?> ConsumerImpl<byte[]>
            // the first message crashes the mailbox; the second is left for the drain
            post consumerImpl.Mb
                (ConsumerMessage.MessageReceived(struct (Unchecked.defaultof<RawMessage>, Unchecked.defaultof<ClientCnx>)))
            post consumerImpl.Mb
                (ConsumerMessage.MessageReceived(struct (queuedMessage, Unchecked.defaultof<ClientCnx>)))

            // GetStats is behind the queued message, so the payload has been discarded once this returns
            Expect.throwsT2<NotConnectedException> (fun () ->
                consumer.GetStats().WaitAsync(TimeSpan.FromSeconds(5.0)).Result |> ignore) |> ignore
            Expect.isFalse "Queued message payload should be disposed when the mailbox stops" payload.CanRead
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Queued messages release their payloads when the consumer mailbox fails")
        }

        testTask "Consumer completes the request that faults its mailbox" {

            Log.Debug("Started Consumer completes the request that faults its mailbox")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingInFlight")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let consumerImpl = consumer :?> ConsumerImpl<byte[]>
            let seek = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            // a null chunk list throws while building the seek, before its channel is completed
            let badSeekId = { MessageId.Earliest with ChunkMessageIds = Some null }
            post consumerImpl.Mb (ConsumerMessage.SeekAsync(SeekType.MessageId badSeekId, seek))

            do! assertMailboxException seek.Task
            Expect.throwsT2<NotConnectedException> (fun () ->
                consumer.GetStats().WaitAsync(TimeSpan.FromSeconds(5.0)).Result |> ignore) |> ignore
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Consumer completes the request that faults its mailbox")
        }

        testTask "Close queued behind a consumer mailbox failure completes only after cleanup" {

            Log.Debug("Started Close queued behind a consumer mailbox failure completes only after cleanup")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingQueuedClose")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let consumerImpl = consumer :?> ConsumerImpl<byte[]>
            // the topic is empty, so this stays a receive waiter until stopConsumer fails it
            let pendingReceive = TaskCompletionSource<Message<byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously)
            post consumerImpl.Mb
                (ConsumerMessage.Receive { CancellationToken = CancellationToken.None; MessageChannel = pendingReceive })

            let trigger = TaskCompletionSource<ConsumerStats>()
            let seek = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let close = TaskCompletionSource<unit>()
            // queued while the trigger is being handled: the seek faults the mailbox and the Close is left for the drain
            queueBehind trigger (fun () ->
                let badSeekId = { MessageId.Earliest with ChunkMessageIds = Some null }
                post consumerImpl.Mb (ConsumerMessage.SeekAsync(SeekType.MessageId badSeekId, seek))
                post consumerImpl.Mb (ConsumerMessage.Close close))
            let atClose =
                snapshotOnCompletion close (fun () ->
                    consumer.IsConnected().Result, seek.Task.IsCompleted, pendingReceive.Task.IsCompleted)
            post consumerImpl.Mb (ConsumerMessage.GetStats trigger)

            let! (connected, seekCompleted, receiveCompleted) = atClose.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "Consumer should be failed before the queued Close completes" connected
            Expect.isTrue "In-flight request should be failed before the queued Close completes" seekCompleted
            Expect.isTrue "Receive waiter should be failed before the queued Close completes" receiveCompleted
            do! assertMailboxException seek.Task
            do! assertAlreadyClosed pendingReceive.Task
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Close queued behind a consumer mailbox failure completes only after cleanup")
        }

        testTask "In-flight message payload is disposed when the consumer mailbox fails" {

            Log.Debug("Started In-flight message payload is disposed when the consumer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingInFlightPayload")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let payload = new System.IO.MemoryStream([| 1uy |])
            let crashingMessage =
                {
                    MessageId = MessageId.Earliest
                    ConsumerEpoch = Nullable()
                    Metadata = Unchecked.defaultof<_>
                    RedeliveryCount = 0
                    Payload = payload
                    MessageKey = null
                    IsKeyBase64Encoded = false
                    CheckSumValid = true
                    Properties = null
                    AckSet = null
                }
            // checksum passes and metadata is missing, so this throws before the handler disposes the payload
            post (consumer :?> ConsumerImpl<byte[]>).Mb
                (ConsumerMessage.MessageReceived(struct (crashingMessage, Unchecked.defaultof<ClientCnx>)))

            Expect.throwsT2<NotConnectedException> (fun () ->
                consumer.GetStats().WaitAsync(TimeSpan.FromSeconds(5.0)).Result |> ignore) |> ignore
            Expect.isFalse "In-flight message payload should be disposed when the mailbox stops" payload.CanRead
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished In-flight message payload is disposed when the consumer mailbox fails")
        }

        testTask "Queued receive is closed when the consumer mailbox fails" {

            Log.Debug("Started Queued receive is closed when the consumer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("failingMailboxQueuedReceive")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            // registered in the waiter list before the mailbox stops, so stopConsumer fails it
            let pendingReceive = consumer.ReceiveAsync()
            do! Task.Delay 200
            post (consumer :?> ConsumerImpl<byte[]>).Mb
                (ConsumerMessage.MessageReceived(struct (Unchecked.defaultof<RawMessage>, Unchecked.defaultof<ClientCnx>)))

            do! assertAlreadyClosed pendingReceive
            do! assertNotConnected (consumer.GetStats())
            let! connected = consumer.IsConnected()
            Expect.isFalse "Consumer should report disconnected after its mailbox stops" connected
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Queued receive is closed when the consumer mailbox fails")
        }

        testTask "Requests posted after the consumer is closed are already closed" {

            Log.Debug("Started Requests posted after the consumer is closed are already closed")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("closedMailboxRequests")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            // public GetStats throws before posting once the consumer is closed; this post reaches the drain
            let stats = TaskCompletionSource<ConsumerStats>(TaskCreationOptions.RunContinuationsAsynchronously)
            post (consumer :?> ConsumerImpl<byte[]>).Mb (ConsumerMessage.GetStats stats)
            do! assertAlreadyClosed stats.Task

            Log.Debug("Finished Requests posted after the consumer is closed are already closed")
        }

        testTask "Multi-topic consumer keeps consuming the other topics when a child mailbox fails" {

            Log.Debug("Started Multi-topic consumer keeps consuming the other topics when a child mailbox fails")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("failingChildMailbox")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let crashed = multiConsumer.Consumers |> Array.head :?> ConsumerImpl<byte[]>
            let remainingTopic = (multiConsumer.Consumers |> Array.last).Topic
            // queued before the child fails, and has to outlive it
            let pendingReceive = consumer.ReceiveAsync()
            do! Task.Delay 200
            crashChildMailbox crashed
            do! waitForStoppedChild crashed

            // as in the Java client, the stopped child is neither recreated nor removed, and the parent keeps running
            Expect.isFalse "A failed child must not complete the pending receive" pendingReceive.IsCompleted
            Expect.equal "The stopped child should stay registered" 2 multiConsumer.Consumers.Length
            let! connected = consumer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "A stopped child is not connected" connected
            // restarts every child's reads, including the stopped child's, which has nothing to redeliver
            do! consumer.RedeliverUnacknowledgedMessagesAsync().WaitAsync(TimeSpan.FromSeconds(5.0))

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(remainingTopic)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync([| 1uy |])
            let! (message : Message<byte[]>) = pendingReceive.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Pending receive should get the message from the remaining child" [| 1uy |] (message.GetValue())
            do! consumer.AcknowledgeAsync(message.MessageId).WaitAsync(TimeSpan.FromSeconds(5.0))
            do! producer.DisposeAsync().AsTask()
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Multi-topic consumer keeps consuming the other topics when a child mailbox fails")
        }

        testTask "Multi-topic consumer completes the request that faults its mailbox" {

            Log.Debug("Started Multi-topic consumer completes the request that faults its mailbox")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("failingInFlightPoll")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let pollerReply = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            crashMultiTopicMailbox (consumer :?> MultiTopicsConsumerImpl<byte[]>) pollerReply
            do! assertMailboxException pollerReply.Task

            // the drain reports why the consumer stopped
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (consumer.GetStats())
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (consumer.LastDisconnectedTimestamp())
            let! connected = consumer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "Multi-topic consumer should report disconnected after its mailbox stops" connected
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Multi-topic consumer completes the request that faults its mailbox")
        }

        testTask "Multi-topic consumer fails only the acknowledgement for an unknown topic" {

            Log.Debug("Started Multi-topic consumer fails only the acknowledgement for an unknown topic")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("unknownTopicAck")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            // e.g. a message from a topic that pattern discovery has removed
            let bogusId = { MessageId.Earliest with TopicName = %"missing-topic" }
            do! assertNotConnected (consumer.AcknowledgeAsync(bogusId))
            do! assertNotConnected (consumer.NegativeAcknowledge(bogusId))
            do! assertNotConnected (consumer.AcknowledgeCumulativeAsync(bogusId))
            let! connected = consumer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isTrue "Multi-topic consumer should stay connected" connected

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName1)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync([| 1uy |])
            let! (message : Message<byte[]>) = consumer.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Multi-topic consumer should keep consuming" [| 1uy |] (message.GetValue())
            do! consumer.AcknowledgeAsync(message.MessageId).WaitAsync(TimeSpan.FromSeconds(5.0))
            do! producer.DisposeAsync().AsTask()
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Multi-topic consumer fails only the acknowledgement for an unknown topic")
        }

        testTask "Child mailbox failure keeps a pending batch receive alive" {
            let client = getClient()
            let topics =
                [| "public/default/topic-" + Guid.NewGuid().ToString("N")
                   "public/default/topic-" + Guid.NewGuid().ToString("N") |]
            use! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics(topics)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(1)
                    .BatchReceivePolicy(BatchReceivePolicy(-1, 1024L, TimeSpan.FromMinutes(1.0)))
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let crashed = multiConsumer.Consumers |> Array.head :?> ConsumerImpl<byte[]>
            let remainingTopic = (multiConsumer.Consumers |> Array.last).Topic
            let pendingBatch = consumer.BatchReceiveAsync()
            do! Task.Delay 200
            crashChildMailbox crashed

            // A buffered close error would fill the one-message queue, so this also checks that it isn't buffered.
            do! waitForStoppedChild crashed
            Expect.isFalse "A failed child must not complete the pending batch" pendingBatch.IsCompleted
            use! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(remainingTopic)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync(Array.create 1024 1uy)
            let! (messages : Messages<byte[]>) = pendingBatch.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Pending batch should get the message from the remaining child" 1 messages.Count
        }

        testTask "Close queued behind a multi-topic consumer mailbox failure completes only after cleanup" {

            Log.Debug("Started Close queued behind a multi-topic consumer mailbox failure completes only after cleanup")
            let client = getClient()
            // nothing matches, so there are no child consumers and the drain completes Close as soon as it reads it
            let pattern = "public/default/topic-" + Guid.NewGuid().ToString("N") + "-.*"

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(pattern)
                    .ConsumerName("failingQueuedMultiClose")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            // nothing can arrive, so this stays a receive waiter until stopConsumer fails it
            let pendingReceive = TaskCompletionSource<Message<byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously)
            post multiConsumer.Mb
                (MultiTopicConsumerMessage.Receive { CancellationToken = CancellationToken.None; MessageChannel = pendingReceive })

            let trigger = TaskCompletionSource<bool>()
            let pollerReply = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let close = TaskCompletionSource<unit>()
            // queued while the trigger is being handled: the null poll result faults the mailbox
            // and the Close is left for the drain
            queueBehind trigger (fun () ->
                crashMultiTopicMailbox multiConsumer pollerReply
                post multiConsumer.Mb (MultiTopicConsumerMessage.Close close))
            let atClose =
                snapshotOnCompletion close (fun () -> pollerReply.Task.IsCompleted, pendingReceive.Task.IsCompleted)
            post multiConsumer.Mb (MultiTopicConsumerMessage.IsConnected trigger)

            let! (pollCompleted, receiveCompleted) = atClose.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isTrue "In-flight request should be failed before the queued Close completes" pollCompleted
            Expect.isTrue "Receive waiter should be failed before the queued Close completes" receiveCompleted
            do! assertMailboxException pollerReply.Task
            do! assertAlreadyClosed pendingReceive.Task
            // a Failed transition after the drain's Close would overwrite Closed and make this NotConnectedException
            do! assertAlreadyClosed (consumer.GetStats())
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Close queued behind a multi-topic consumer mailbox failure completes only after cleanup")
        }

        testTask "Multi-topic consumer closes its children at the broker after a child mailbox fails" {

            Log.Debug("Started Multi-topic consumer closes its children at the broker after a child mailbox fails")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "test-subscription"

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("failingChildBrokerClose")
                    .SubscriptionName(subscriptionName)
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let crashed = multiConsumer.Consumers |> Array.head :?> ConsumerImpl<byte[]>
            crashChildMailbox crashed
            do! waitForStoppedChild crashed
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            // the crashed child closed itself at the broker and Close closed the other one, so both topics take
            // a new exclusive consumer
            for topicName in [ topicName1; topicName2 ] do
                let! (consumer2 : IConsumer<byte[]>) =
                    client.NewConsumer()
                        .Topic(topicName)
                        .ConsumerName("replacement")
                        .SubscriptionName(subscriptionName)
                        .SubscriptionType(SubscriptionType.Exclusive)
                        .SubscribeAsync()
                        .WaitAsync(TimeSpan.FromSeconds(15.0))
                do! consumer2.UnsubscribeAsync()

            Log.Debug("Finished Multi-topic consumer closes its children at the broker after a child mailbox fails")
        }

        testTask "A pattern topic discovered after a redelivery is consumed" {

            Log.Debug("Started A pattern topic discovered after a redelivery is consumed")
            let client = getClient()
            let topicPrefix = $"public/default/pattern-epoch-{Guid.NewGuid():N}-"
            let! (firstProducer : IProducer<byte[]>) =
                client.NewProducer().Topic(topicPrefix + "first").EnableBatching(false).CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(topicPrefix + ".*")
                    .ConsumerName("discoveredAfterRedelivery")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromHours(1.0))
                    .SubscribeAsync()
            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            // moves the consumer epoch on. A child created afterwards has to start from it, otherwise the parent
            // discards its messages as stale
            do! consumer.RedeliverUnacknowledgedMessagesAsync().WaitAsync(TimeSpan.FromSeconds(5.0))

            let! (secondProducer : IProducer<byte[]>) =
                client.NewProducer().Topic(topicPrefix + "second").EnableBatching(false).CreateAsync()
            post multiConsumer.Mb MultiTopicConsumerMessage.PatternTickTime
            do! waitUntil "the new topic is subscribed" (fun () -> task {
                let! connected = consumer.IsConnected()
                return connected && multiConsumer.Consumers.Length = 2
            })

            let! _ = secondProducer.SendAsync([| 1uy |])
            let! (message : Message<byte[]>) = consumer.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The discovered topic should be consumed" [| 1uy |] (message.GetValue())
            do! consumer.AcknowledgeAsync(message.MessageId).WaitAsync(TimeSpan.FromSeconds(5.0))
            do! firstProducer.DisposeAsync().AsTask()
            do! secondProducer.DisposeAsync().AsTask()
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished A pattern topic discovered after a redelivery is consumed")
        }

        testTask "Multi-topic consumer can't seek or unsubscribe while a child has stopped" {

            Log.Debug("Started Multi-topic consumer can't seek or unsubscribe while a child has stopped")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("stoppedChildRequests")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let crashed = multiConsumer.Consumers |> Array.head :?> ConsumerImpl<byte[]>
            let remainingTopic = (multiConsumer.Consumers |> Array.last).Topic
            crashChildMailbox crashed
            do! waitForStoppedChild crashed

            // as in the Java client, the request goes to every child, so the stopped one fails it
            do! assertNotConnected (consumer.SeekAsync(MessageId.Earliest))

            // the consumer keeps running
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(remainingTopic)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync([| 1uy |])
            let! (message : Message<byte[]>) = consumer.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The remaining topic should still be consumed" [| 1uy |] (message.GetValue())
            do! consumer.AcknowledgeAsync(message.MessageId).WaitAsync(TimeSpan.FromSeconds(5.0))
            do! producer.DisposeAsync().AsTask()

            do! assertNotConnected (consumer.UnsubscribeAsync())
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Multi-topic consumer can't seek or unsubscribe while a child has stopped")
        }

        testTask "A stopped child doesn't dispose the multi-topic consumer's interceptors" {

            Log.Debug("Started A stopped child doesn't dispose the multi-topic consumer's interceptors")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interceptor = new CountingInterceptor()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("stoppedChildInterceptors")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .Intercept(interceptor)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let crashed = multiConsumer.Consumers |> Array.head :?> ConsumerImpl<byte[]>
            let remainingTopic = (multiConsumer.Consumers |> Array.last).Topic
            crashChildMailbox crashed
            do! waitForStoppedChild crashed
            Expect.equal "A stopped child must not dispose the interceptors the parent still uses" 0 interceptor.Disposed

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(remainingTopic)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync([| 1uy |])
            let! (message : Message<byte[]>) = consumer.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The remaining child should still run the interceptors" 1 interceptor.Consumed
            do! consumer.AcknowledgeAsync(message.MessageId).WaitAsync(TimeSpan.FromSeconds(5.0))
            do! producer.DisposeAsync().AsTask()
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Closing the parent should dispose the interceptors once" 1 interceptor.Disposed

            Log.Debug("Finished A stopped child doesn't dispose the multi-topic consumer's interceptors")
        }

        testTask "Multi-topic consumer keeps its interceptors until its children are closed after its mailbox fails" {

            Log.Debug("Started Multi-topic consumer keeps its interceptors until its children are closed after its mailbox fails")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interceptor = new CountingInterceptor()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("failedParentInterceptors")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .Intercept(interceptor)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let pollerReply = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            crashMultiTopicMailbox multiConsumer pollerReply
            do! assertMailboxException pollerReply.Task
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (consumer.GetStats())
            Expect.equal "The children still use the interceptors after the mailbox fails" 0 interceptor.Disposed

            // the children outlive the mailbox until Close
            let child = multiConsumer.Consumers |> Array.head
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(child.Topic)
                    .EnableBatching(false)
                    .CreateAsync()
            let! _ = producer.SendAsync([| 1uy |])
            let! (_ : Message<byte[]>) = child.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The child should still run the interceptors" 1 interceptor.Consumed
            do! producer.DisposeAsync().AsTask()

            // overlapping Closes, e.g. from concurrent disposals: only the first one closes the children
            // and then disposes the interceptors
            let closes = Array.init 2 (fun _ -> TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously))
            for close in closes do
                post multiConsumer.Mb (MultiTopicConsumerMessage.Close close)
            let! _ = (closes |> Array.map _.Task |> Task.WhenAll).WaitAsync(TimeSpan.FromSeconds(5.0))
            for child in multiConsumer.Consumers do
                do! assertAlreadyClosed (child.GetStats())
            Expect.equal "Closing the multi-topic consumer should dispose the interceptors once" 1 interceptor.Disposed
            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Multi-topic consumer keeps its interceptors until its children are closed after its mailbox fails")
        }

        testTask "Closing the client closes a multi-topic consumer whose mailbox failed" {

            Log.Debug("Started Closing the client closes a multi-topic consumer whose mailbox failed")
            let! (client : PulsarClient) = PulsarClientBuilder().ServiceUrl(pulsarAddress).BuildAsync()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interceptor = new CountingInterceptor()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("failedParentClientClose")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .Intercept(interceptor)
                    .SubscribeAsync()

            let multiConsumer = consumer :?> MultiTopicsConsumerImpl<byte[]>
            let pollerReply = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            crashMultiTopicMailbox multiConsumer pollerReply
            do! assertMailboxException pollerReply.Task
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (consumer.GetStats())

            // as in the Java client, the failed consumer stays registered until it is closed, so closing the client
            // closes its children and disposes the interceptors
            do! client.CloseAsync().WaitAsync(TimeSpan.FromSeconds(10.0))
            for child in multiConsumer.Consumers do
                do! assertAlreadyClosed (child.GetStats())
            Expect.equal "Closing the client should dispose the interceptors" 1 interceptor.Disposed
            do! assertAlreadyClosed (consumer.GetStats())

            Log.Debug("Finished Closing the client closes a multi-topic consumer whose mailbox failed")
        }

        testTask "Removing a pattern topic ends the reads of its failed consumer" {

            Log.Debug("Started Removing a pattern topic ends the reads of its failed consumer")
            let topicPrefix = $"persistent://public/default/removed-failed-child-{Guid.NewGuid():N}-"
            let removedTopic = TopicName(topicPrefix + "removed")
            let remainingTopic = TopicName(topicPrefix + "remaining")
            let removedName : string = %removedTopic.CompleteTopicName
            // pattern children never create topics
            for topic in [ removedTopic; remainingTopic ] do
                let! (producer : IProducer<byte[]>) = getClient().NewProducer().Topic(topic.ToString()).CreateAsync()
                do! producer.DisposeAsync().AsTask()
            let retrying = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let readEnded = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            do! runWithLogger (fun text ->
                    if text.Contains("receive failed on the consumer for " + removedName) then
                        retrying.TrySetResult() |> ignore
                    if text.Contains("stopped reading the removed consumer for " + removedName) then
                        readEnded.TrySetResult() |> ignore) (fun () -> task {
                let config = { PulsarClientConfiguration.Default with ServiceAddresses = [| Uri(pulsarAddress) |] }
                let pool = ConnectionPool(config)
                let lookup = new BinaryLookupService(config, pool) :> ILookupService
                try
                    let consumerInfo topic : ConsumerInitInfo<byte[]> =
                        { TopicName = topic; Schema = Schema.BYTES(); SchemaProvider = None; Metadata = { Partitions = 0 } }
                    let patternInfo =
                        { InitialTopics = [| consumerInfo removedTopic; consumerInfo remainingTopic |]
                          GetTopics = fun () -> Task.FromResult [| remainingTopic |]
                          GetConsumerInfo = consumerInfo >> Task.FromResult }
                    let consumerConfig =
                        { ConsumerConfiguration<byte[]>.Default with
                            TopicsPattern = topicPrefix + ".*"
                            SubscriptionName = %"removed-failed-child"
                            PatternAutoDiscoveryPeriod = TimeSpan.FromHours(1.0) }
                    let! (multiConsumer : MultiTopicsConsumerImpl<byte[]>) =
                        MultiTopicsConsumerImpl.InitPattern(consumerConfig, config, pool, patternInfo, lookup,
                                                            ConsumerInterceptors<byte[]>.Empty, ignore)
                    let consumer = multiConsumer :> IConsumer<byte[]>
                    let failed =
                        multiConsumer.Consumers |> Array.find (fun child -> child.Topic = removedName) :?> ConsumerImpl<byte[]>
                    crashChildMailbox failed
                    do! waitForStoppedChild failed
                    // restarts every child's reads, so the failed child's read is retried every 10 seconds
                    do! consumer.RedeliverUnacknowledgedMessagesAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
                    do! retrying.Task.WaitAsync(TimeSpan.FromSeconds(5.0))

                    // discovery removes the topic, which ends the read instead of leaving it to retry
                    post multiConsumer.Mb MultiTopicConsumerMessage.PatternTickTime
                    do! readEnded.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                    // the read ends before discovery has removed the child, so wait for the mailbox to finish discovery
                    let! _ = consumer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
                    Expect.equal "Only the remaining topic's consumer should be left" 1 multiConsumer.Consumers.Length
                    do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))
                finally
                    lookup.Dispose()
                    pool.CloseAllConnections().GetAwaiter().GetResult()
            })

            Log.Debug("Finished Removing a pattern topic ends the reads of its failed consumer")
        }

        testTask "Requests posted after the multi-topic consumer is closed are already closed" {

            Log.Debug("Started Requests posted after the multi-topic consumer is closed are already closed")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([| topicName1; topicName2 |])
                    .ConsumerName("closedMultiMailboxRequests")
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscribeAsync()

            do! consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            // GetStats still posts after close, and the drain answers from the closed state
            do! assertAlreadyClosed (consumer.GetStats())

            Log.Debug("Finished Requests posted after the multi-topic consumer is closed are already closed")
        }

        testTask "Producer is closed at the broker when its mailbox fails" {

            Log.Debug("Started Producer is closed at the broker when its mailbox fails")
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            // Only this client's connection should count towards the close-reply assertion.
            let observed = obj()
            let mutable producerId = ""
            let mutable cnxPrefix = ""
            let missingCloseRequests = ConcurrentQueue<string>()
            do! runWithLogger (fun text ->
                lock observed (fun () ->
                    if producerId = "" && text.Contains(topicName) && text.Contains("starting register") then
                        let matched = System.Text.RegularExpressions.Regex.Match(text, @"producer\((\d+),")
                        if matched.Success then
                            producerId <- matched.Groups[1].Value
                    if cnxPrefix = "" && producerId <> "" && text.EndsWith(" adding producer " + producerId) then
                        let marker = " adding producer " + producerId
                        cnxPrefix <- text.Substring(0, text.Length - marker.Length)
                    if cnxPrefix <> "" && text.StartsWith(cnxPrefix) && text.Contains("complete non-existent request") then
                        missingCloseRequests.Enqueue(text))) (fun () -> task {
                    let client = getNewClient()
                    try
                        let producerName = "failingMailbox"

                        let! (producer : IProducer<byte[]>) =
                            client.NewProducer()
                                .Topic(topicName)
                                .ProducerName(producerName)
                                .CreateAsync()

                        let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
                        // a null message crashes the producer mailbox while handling the send
                        post (producer :?> ProducerImpl<byte[]>).Mb
                            (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))

                        let crashed =
                            task {
                                try
                                    let! _ = crashingSend.Task
                                    return false
                                with _ ->
                                    return true
                            }
                        let! sendFailed = crashed.WaitAsync(TimeSpan.FromSeconds(5.0))
                        if not sendFailed then
                            failwith "Crashing send did not fail within 5 seconds"

                        let deadline = DateTime.UtcNow.AddSeconds 5.0
                        let mutable connected = true
                        while connected do
                            if DateTime.UtcNow >= deadline then
                                failwith "Producer did not fail within 5 seconds"
                            let! isConnected = producer.IsConnected()
                            if isConnected then
                                do! Task.Delay 100
                            connected <- isConnected

                        // SendAsync checks the producer state before it posts, and still reports why the producer failed
                        do! assertNotConnectedBecauseOf typeof<NullReferenceException> (task { return! producer.SendAsync([| 1uy |]) })

                        // GetStats still posts once the producer has failed, so this only returns if the stopped mailbox replies
                        do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.GetStats())

                        do! producer.DisposeAsync()

                        // the producer name must be free for a replacement. That reply is the broker Success
                        // for CloseProducer, so an unregistered request id would already have been logged
                        let! (producer2 : IProducer<byte[]>) =
                            client.NewProducer()
                                .Topic(topicName)
                                .ProducerName(producerName)
                                .CreateAsync()
                                .WaitAsync(TimeSpan.FromSeconds(15.0))
                        do! producer2.DisposeAsync()

                        if cnxPrefix = "" then
                            failwith "Did not observe the producer connection, so an unregistered CloseProducer reply would go unnoticed"
                        if not missingCloseRequests.IsEmpty then
                            failwith ("CloseProducer reply was not registered: " + String.Join(" | ", missingCloseRequests))
                    finally
                        client.CloseAsync().GetAwaiter().GetResult()
                })

            Log.Debug("Finished Producer is closed at the broker when its mailbox fails")
        }

        testTask "Pending broker sends fault when the producer mailbox fails" {

            Log.Debug("Started Pending broker sends fault when the producer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let mutable producerImpl = Unchecked.defaultof<ProducerImpl<byte[]>>
            let pendingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            // encryption runs inside the send, before the mailbox reads again. The crash is queued there
            // so it is processed only after the send is stored as a pending broker message, and a broker
            // ack cannot overtake it on this single-reader channel
            let encryptor =
                { new IMessageEncryptor with
                    member _.Encrypt payload =
                        post producerImpl.Mb
                            (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))
                        EncryptedMessage(payload, Array.empty, "", Array.empty)
                    member _.UpdateEncryptionKeys() = () }

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MessageEncryptor(encryptor)
                    .CreateAsync()
            producerImpl <- producer :?> ProducerImpl<byte[]>

            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (producer.NewMessage([| 1uy |]), pendingSend, false)))

            do! assertMailboxException pendingSend.Task
            do! assertMailboxException crashingSend.Task
            do! producer.DisposeAsync()

            Log.Debug("Finished Pending broker sends fault when the producer mailbox fails")
        }

        testTask "Batched sends fault when the producer mailbox fails" {

            Log.Debug("Started Batched sends fault when the producer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(10)
                    .BatchingMaxPublishDelay(TimeSpan.FromMinutes(1.0))
                    .CreateAsync()

            let producerImpl = producer :?> ProducerImpl<byte[]>
            let batchedSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            // one message stays in the batch container: the batch is neither full nor due
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (producer.NewMessage([| 1uy |]), batchedSend, false)))
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))

            do! assertMailboxException batchedSend.Task
            do! assertMailboxException crashingSend.Task
            do! producer.DisposeAsync()

            Log.Debug("Finished Batched sends fault when the producer mailbox fails")
        }

        testTask "All key batches fault when a later batch crashes the producer mailbox" {

            Log.Debug("Started All key batches fault when a later batch crashes the producer mailbox")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let failureMessage = "replication clusters enumeration failed"
            let failingReplicationClusters =
                seq {
                    raise (InvalidOperationException failureMessage)
                    yield ""
                }

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchBuilder(BatchBuilder.KeyBased)
                    .BatchingMaxMessages(10)
                    .BatchingMaxPublishDelay(TimeSpan.FromMinutes(1.0))
                    .CreateAsync()

            let firstSend = producer.SendAsync(producer.NewMessage([| 1uy |], key = "first"))
            let secondMessage =
                producer.NewMessage([| 2uy |], key = "second")
                    .WithReplicateTo(failingReplicationClusters)
            let secondSend = producer.SendAsync(secondMessage)

            // Key batches are processed by increasing sequence id. The first is added to
            // pendingMessages, then enumerating the second batch's replication clusters throws.
            // Both callbacks still remain in the uncleared key-batch container at that point.
            let flush = producer.FlushAsync()

            let assertBatchFailure (work: Task<'T>) = task {
                try
                    let! _ = work.WaitAsync(TimeSpan.FromSeconds(5.0))
                    failwith "Task completed successfully after the key batch crashed the mailbox"
                with ex ->
                    match unwrap ex with
                    | :? InvalidOperationException as failure when failure.Message = failureMessage -> ()
                    | :? TimeoutException -> failwith "Task did not fault within 5 seconds"
                    | other -> failwith $"Expected the key batch exception but got {other.GetType().Name}: {other.Message}"
            }

            do! assertBatchFailure firstSend
            do! assertBatchFailure secondSend
            do! assertBatchFailure flush
            do! producer.DisposeAsync()

            Log.Debug("Finished All key batches fault when a later batch crashes the producer mailbox")
        }

        testTask "BlockIfQueueFull sends fault when the producer mailbox fails" {

            Log.Debug("Started BlockIfQueueFull sends fault when the producer mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .BlockIfQueueFull(true)
                    .MaxPendingMessages(0)
                    .CreateAsync()

            let producerImpl = producer :?> ProducerImpl<byte[]>
            let blockedSend1 = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            let blockedSend2 = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            // MaxPendingMessages 0 accepts nothing, so both sends sit in the blocked queue.
            // a null send would be blocked too, so crash through a different mailbox message
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (producer.NewMessage([| 1uy |]), blockedSend1, false)))
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (producer.NewMessage([| 2uy |]), blockedSend2, false)))
            post producerImpl.Mb
                (ProducerMessage.GetStats(Unchecked.defaultof<TaskCompletionSource<ProducerStats>>))

            do! assertMailboxException blockedSend1.Task
            do! assertMailboxException blockedSend2.Task
            do! producer.DisposeAsync()

            Log.Debug("Finished BlockIfQueueFull sends fault when the producer mailbox fails")
        }

        testTask "Requests posted as the producer mailbox fails are faulted" {

            Log.Debug("Started Requests posted as the producer mailbox fails are faulted")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let producerImpl = producer :?> ProducerImpl<byte[]>
            let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            // the mailbox loop throws inside this post. Later posts are read only by the drain
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))

            let lateSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            let flush = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let close = TaskCompletionSource<Result<unit, exn>>(TaskCreationOptions.RunContinuationsAsynchronously)
            let stats = TaskCompletionSource<ProducerStats>(TaskCreationOptions.RunContinuationsAsynchronously)
            post producerImpl.Mb
                (ProducerMessage.BeginSendMessage(struct (producer.NewMessage([| 1uy |]), lateSend, false)))
            post producerImpl.Mb (ProducerMessage.Flush flush)
            post producerImpl.Mb (ProducerMessage.Close close)
            post producerImpl.Mb (ProducerMessage.GetStats stats)

            do! assertMailboxException crashingSend.Task
            // these were still in the mailbox when it stopped, so the drain completes them
            do! assertNotConnected lateSend.Task
            do! assertNotConnected flush.Task
            do! assertNotConnected stats.Task
            let! (closeResult: Result<unit, exn>) = close.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
            match closeResult with
            | Ok () -> ()
            | Error other -> failwith $"Close failed after mailbox failure with {other.GetType().Name}: {other.Message}"

            do! producer.DisposeAsync()
            Log.Debug("Finished Requests posted as the producer mailbox fails are faulted")
        }

        testTask "Close queued behind a producer mailbox failure completes only after cleanup" {

            Log.Debug("Started Close queued behind a producer mailbox failure completes only after cleanup")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let producerImpl = producer :?> ProducerImpl<byte[]>
            let trigger = TaskCompletionSource<ProducerStats>()
            let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            let close = TaskCompletionSource<Result<unit, exn>>()
            // queued while the trigger is being handled: the null send faults the mailbox and the Close is left for the drain
            queueBehind trigger (fun () ->
                post producerImpl.Mb
                    (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))
                post producerImpl.Mb (ProducerMessage.Close close))
            let atClose =
                snapshotOnCompletion close (fun () -> producer.IsConnected().Result, crashingSend.Task.IsCompleted)
            post producerImpl.Mb (ProducerMessage.GetStats trigger)

            let! (connected, sendCompleted) = atClose.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "Producer should be failed before the queued Close completes" connected
            Expect.isTrue "In-flight request should be failed before the queued Close completes" sendCompleted
            do! assertMailboxException crashingSend.Task
            match close.Task.Result with
            | Ok () -> ()
            | Error other -> failwith $"Close failed after mailbox failure with {other.GetType().Name}: {other.Message}"
            do! producer.DisposeAsync()

            Log.Debug("Finished Close queued behind a producer mailbox failure completes only after cleanup")
        }

        testTask "Partitioned producer keeps sending to the other partitions when a partition producer fails" {

            Log.Debug("Started Partitioned producer keeps sending to the other partitions when a partition producer fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            do! createPartitionedTopic topicName 2
            let interceptor = new CountingProducerInterceptor()
            // every message goes to the partition that keeps working
            let router = { new IMessageRouter with member _.ChoosePartition(_, _) = 1 }

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("failingPartition")
                    .MessageRoutingMode(MessageRoutingMode.CustomPartition)
                    .CustomMessageRouter(router)
                    .Intercept(interceptor)
                    .CreateAsync()

            let partitioned = producer :?> PartitionedProducerImpl<byte[]>
            let failed = partitioned.Producers |> Array.head :?> ProducerImpl<byte[]>
            let crashingSend = TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously)
            // a null message crashes one partition mailbox
            post failed.Mb
                (ProducerMessage.BeginSendMessage(struct (Unchecked.defaultof<MessageBuilder<byte[]>>, crashingSend, false)))
            do! assertMailboxException crashingSend.Task

            // as in the Java client, a failed partition doesn't stop the others
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.GetStats())
            let! (_ : SequenceId) = producer.LastSequenceId().WaitAsync(TimeSpan.FromSeconds(5.0))
            let! connected = producer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "The failed partition is not connected" connected
            let! (_ : MessageId) = producer.SendAsync([| 1uy |]).WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The failed partition must not dispose the interceptors the others still use" 0 interceptor.Disposed
            Expect.equal "The remaining partition should still run the interceptors" 1 interceptor.Sent
            do! producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Closing the partitioned producer should dispose the interceptors once" 1 interceptor.Disposed

            // the partition producers were closed, so the producer name is free
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("failingPartition")
                    .CreateAsync()
                    .WaitAsync(TimeSpan.FromSeconds(15.0))
            do! producer2.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Partitioned producer keeps sending to the other partitions when a partition producer fails")
        }

        testTask "Partitioned producer reports why its mailbox failed" {

            Log.Debug("Started Partitioned producer reports why its mailbox failed")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            do! createPartitionedTopic topicName 2

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("failingPartitionedMailbox")
                    .CreateAsync()

            crashPartitionedProducerMailbox (producer :?> PartitionedProducerImpl<byte[]>)
            // answered by the drain, with the failure as the cause
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.GetStats())
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.LastSequenceId())
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.LastDisconnectedTimestamp())
            // sends never reach the mailbox, they check the producer state first
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (task { return! producer.SendAsync([| 1uy |]) })
            let! connected = producer.IsConnected().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse "Partitioned producer should report disconnected after its mailbox stops" connected
            do! producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            // the partition producers outlive the mailbox and were closed by Close, so the producer name is free
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("failingPartitionedMailbox")
                    .CreateAsync()
                    .WaitAsync(TimeSpan.FromSeconds(15.0))
            do! producer2.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Partitioned producer reports why its mailbox failed")
        }

        testTask "Partitioned producer keeps its interceptors until its partitions are closed after its mailbox fails" {

            Log.Debug("Started Partitioned producer keeps its interceptors until its partitions are closed after its mailbox fails")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            do! createPartitionedTopic topicName 2
            let interceptor = new CountingProducerInterceptor()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .Intercept(interceptor)
                    .CreateAsync()

            let partitioned = producer :?> PartitionedProducerImpl<byte[]>
            crashPartitionedProducerMailbox partitioned
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.GetStats())
            Expect.equal "The partitions still use the interceptors after the mailbox fails" 0 interceptor.Disposed

            // the partitions outlive the mailbox until Close, and report the outcome of their sends
            let partition = partitioned.Producers |> Array.head
            let! (_ : MessageId) = partition.SendAsync([| 1uy |]).WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The partition should still report its acknowledgements" 1 interceptor.Acknowledged
            Expect.equal "The interceptors should not be disposed before the partitions are closed" 0 interceptor.Disposed

            // overlapping Closes, e.g. from concurrent disposals: only the first one closes the partitions
            // and then disposes the interceptors
            let closes =
                Array.init 2 (fun _ -> TaskCompletionSource<Result<unit, exn>>(TaskCreationOptions.RunContinuationsAsynchronously))
            for close in closes do
                post partitioned.Mb (PartitionedProducerMessage.Close close)
            let! (closeResults : Result<unit, exn>[]) =
                (closes |> Array.map _.Task |> Task.WhenAll).WaitAsync(TimeSpan.FromSeconds(5.0))
            for closeResult in closeResults do
                match closeResult with
                | Ok () -> ()
                | Error other -> failwith $"Close failed with {other.GetType().Name}: {other.Message}"
            for child in partitioned.Producers do
                do! assertAlreadyClosed (child.GetStats())
            Expect.equal "Closing the partitioned producer should dispose the interceptors once" 1 interceptor.Disposed
            do! producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Partitioned producer keeps its interceptors until its partitions are closed after its mailbox fails")
        }

        testTask "Closing the client closes a partitioned producer whose mailbox failed" {

            Log.Debug("Started Closing the client closes a partitioned producer whose mailbox failed")
            let! (client : PulsarClient) = PulsarClientBuilder().ServiceUrl(pulsarAddress).BuildAsync()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            do! createPartitionedTopic topicName 2
            let interceptor = new CountingProducerInterceptor()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .Intercept(interceptor)
                    .CreateAsync()

            let partitioned = producer :?> PartitionedProducerImpl<byte[]>
            crashPartitionedProducerMailbox partitioned
            do! assertNotConnectedBecauseOf typeof<NullReferenceException> (producer.GetStats())

            // as in the Java client, the failed producer stays registered until it is closed, so closing the client
            // closes its partitions and disposes the interceptors
            do! client.CloseAsync().WaitAsync(TimeSpan.FromSeconds(10.0))
            for child in partitioned.Producers do
                do! assertAlreadyClosed (child.GetStats())
            Expect.equal "Closing the client should dispose the interceptors" 1 interceptor.Disposed
            do! assertAlreadyClosed (producer.GetStats())

            Log.Debug("Finished Closing the client closes a partitioned producer whose mailbox failed")
        }

        testTask "Close queued behind a partitioned producer mailbox failure completes only after cleanup" {

            Log.Debug("Started Close queued behind a partitioned producer mailbox failure completes only after cleanup")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            do! createPartitionedTopic topicName 2

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let partitioned = producer :?> PartitionedProducerImpl<byte[]>
            let trigger = TaskCompletionSource<bool>()
            let close = TaskCompletionSource<Result<unit, exn>>()
            // queued while the trigger is being handled: the first request faults the mailbox and the Close is left
            // for the drain
            queueBehind trigger (fun () ->
                crashPartitionedProducerMailbox partitioned
                post partitioned.Mb (PartitionedProducerMessage.Close close))
            post partitioned.Mb (PartitionedProducerMessage.IsConnected trigger)

            let! (closeResult : Result<unit, exn>) = close.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
            match closeResult with
            | Ok () -> ()
            | Error other -> failwith $"Close failed after mailbox failure with {other.GetType().Name}: {other.Message}"
            // a Failed transition after the drain's Close would overwrite Closed and make this NotConnectedException
            do! assertAlreadyClosed (producer.GetStats())
            for child in partitioned.Producers do
                do! assertAlreadyClosed (child.GetStats())
            do! producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            Log.Debug("Finished Close queued behind a partitioned producer mailbox failure completes only after cleanup")
        }

        testTask "Sent message/messageId should be equal to received message/messageId" {

            Log.Debug("Started Sent messageId should be equal to received messageId")
            let client = getClient()
            let topicName = "persistent://public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("producer1")
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("producer2")
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let! msg1Id = producer1.SendAsync([| 0uy |])
            let! msg2Id = producer2.SendAsync([| 1uy |])
            let! (msg1 : Message<byte[]>) = consumer.ReceiveAsync()
            let! (msg2 : Message<byte[]>) = consumer.ReceiveAsync()

            Expect.isTrue "" (msg1Id = msg1.MessageId)
            Expect.equal "" [| 0uy |] <| msg1.GetValue()
            Expect.equal "Message ID topic name should match" topicName (string msg1.MessageId.TopicName)
            Expect.equal "Message producer name should match" "producer1" (string msg1.ProducerName)

            Expect.isTrue "" (msg2Id = msg2.MessageId)
            Expect.equal "" [| 1uy |] <| msg2.GetValue()
            Expect.equal "Message ID topic name should match" topicName (string msg2.MessageId.TopicName)
            Expect.equal "Message producer name should match" "producer2" (string msg2.ProducerName)

            Log.Debug("Finished Sent messageId should be equal to received messageId")
        }

        testTask "Send and receive 100 messages concurrently works fine in default configuration" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages "concurrent"
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine in default configuration")
        }

        testTask "Send 100 messages and then receiving them works fine when retention is set on namespace" {

            Log.Debug("Started send 100 messages and then receiving them works fine when retention is set on namespace")
            let client = getClient()
            let topicName = "public/retention/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync()

            do! produceMessages producer 100 "sequential"

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("sequential")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync()

            do! consumeMessages consumer 100 "sequential"
            Log.Debug("Finished send 100 messages and then receiving them works fine when retention is set on namespace")
        }

        testTask "Full roundtrip (emulate Request-Response behaviour)" {

            Log.Debug("Started Full roundtrip (emulate Request-Response behaviour)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! (consumer1 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName2)
                    .ConsumerName("consumer1")
                    .SubscriptionName("my-subscriptionx")
                    .SubscribeAsync()

            let! (consumer2 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName1)
                    .ConsumerName("consumer2")
                    .SubscriptionName("my-subscriptiony")
                    .SubscribeAsync()

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName("producer1")
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName2)
                    .ProducerName("producer2")
                    .CreateAsync()

            let t1 = Task.Run(fun () ->
                fastProduceMessages producer1 messagesNumber "producer1" |> Task.WaitAll
                Log.Debug("t1 ended")
            )

            let t2 = Task.Run(fun () ->
                consumeMessages consumer1 messagesNumber "consumer1" |> Task.WaitAll
                Log.Debug("t2 ended")
            )

            let t3 = Task.Run(fun () ->
                task {
                    for i in 1..messagesNumber do
                        let! message = consumer2.ReceiveAsync()
                        let received = Encoding.UTF8.GetString(message.Data)
                        do! consumer2.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} received {1}", "consumer2", received)
                        let expected = "Message #" + string i
                        if received.StartsWith(expected) |> not then
                            failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "consumer2"
                        let! _ = producer2.SendAndForgetAsync(message.Data)
                        ()
                } :> Task
            )
            do! [|t1; t2; t3|] |> Task.WhenAll

            Log.Debug("Finished Full roundtrip (emulate Request-Response behaviour)")
        }

        testTask "Concurrent send and receive work fine" {

            Log.Debug("Started Concurrent send and receive work fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100
            let producerName = "concurrentProducer"
            let consumerName = "concurrentConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTasks =
                [| 1..3 |]
                |> Array.map (fun _ ->
                    Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages producerName
                        } :> Task))

            let mutable processedCount = 0
            let consumerTask i =
                fun () ->
                    task {
                        while true do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0}-{1} received {2}", consumerName, i, received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            Log.Debug("{0}-{1} acknowledged {2}", consumerName, i, received)
                            if Interlocked.Increment(&processedCount) = (numberOfMessages*3) then
                                do! consumer.DisposeAsync()
                    } :> Task
            let consumerTasks =
                [| 1..3 |]
                |> Array.map (fun i -> Task.Run(consumerTask i))

            let resultTasks = Array.append consumerTasks producerTasks
            try
                do! Task.WhenAll(resultTasks)
            with
            | :? AlreadyClosedException ->
                ()
            | ex ->
                failtestf "Incorrect exception type %A" (ex.GetType().FullName)
            Log.Debug("Finished Concurrent send and receive work fine")
        }

        testTask "Client, producer and consumer can't be accessed after close" {

            Log.Debug("Started 'Client, producer and consumer can't be accessed after close'")

            let client = getNewClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer1 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing1-subscription")
                    .SubscribeAsync()

            let! (consumer2 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing2-subscription")
                    .SubscribeAsync()

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer1")
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer2")
                    .CreateAsync()

            do! consumer1.DisposeAsync().AsTask()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer1.ReceiveAsync().Result |> ignore) |> ignore
            do! producer1.DisposeAsync().AsTask()
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAndForgetAsync([||]).Result) |> ignore
            do! client.CloseAsync()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAndForgetAsync([||]).Result) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.CloseAsync().Result) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
        }

        testTask "Scheduled message should be delivered at requested time" {

            Log.Debug("Started 'Scheduled message should be delivered at requested time'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interval = 10000L
            let producerName = "schedule-producer"
            let consumerName = "schedule-consumer"
            let testEventTime = DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) |> convertToMsTimestamp
            let sw = Stopwatch()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName).EnableBatching(false)
                    .ProducerName(producerName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("schedule-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let now = DateTime.UtcNow;
                        let deliverAt = now.AddMilliseconds(float interval) |> convertToMsTimestamp
                        let timestamp = Nullable(%deliverAt)
                        let message = Encoding.UTF8.GetBytes(sprintf "Message was sent with interval '%i' milliseconds" interval)
                        sw.Start()
                        let! _ = producer.NewMessage(message, deliverAt = timestamp, eventTime = (%testEventTime |> Nullable)) |> producer.SendAsync
                        ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! message = consumer.ReceiveAsync()
                        let received = Encoding.UTF8.GetString(message.Data)
                        Log.Debug("{0} received {1}", consumerName, received)
                        Expect.equal "" %testEventTime (message.EventTime.GetValueOrDefault())
                        sw.Stop()
                        do! consumer.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} acknowledged {1}", consumerName, received)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            let elapsed = sw.ElapsedMilliseconds

            interval - elapsed
            |> Math.Abs
            |> (>) 4000L
            |> Expect.isTrue (sprintf "Message delivered in unexpected interval %i while should be %i" elapsed interval)

            Log.Debug("Finished 'Scheduled message should be delivered at requested time'")
        }

        testTask "Create the replicated subscription should be successful" {
            Log.Debug("Started 'Create the replicated subscription should be successful'")
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "replicated-consumer"
            let client = getClient()
            let! (_ : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("replicate")
                    .SubscriptionType(SubscriptionType.Shared)
                    .ReplicateSubscriptionState(true)
                    .SubscribeAsync()

            do! Task.Delay 1000

            let url = $"{pulsarHttpAddress}/admin/v2/persistent/" + topicName + "/stats"
            let! (response: string) = commonHttpClient.GetStringAsync(url)
            let json = JsonDocument.Parse(response)
            let isReplicated = json.RootElement.GetProperty("subscriptions").GetProperty("replicate").GetProperty("isReplicated").GetBoolean()
            Expect.isTrue "" isReplicated
            Log.Debug("Finished 'Create the replicated subscription should be successful'")
        }
        
        testTask "Delete topic subscribed by the pattern consumer should not throw error or recreate topic" {
            Log.Debug("Started 'Delete topic subscribed by the pattern consumer should not throw error or recreate topic'")
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let client = getClient()

            let pattern = topicName + "-.*"
            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName + "-1")
                    .CreateAsync()
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName + "-2")
                    .CreateAsync()
            
            do! producer1.SendAsync([| 0uy |])
            do! producer2.SendAsync([| 0uy |])
                    
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(pattern)
                    .ConsumerName("test")
                    .SubscriptionName("test")
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .ReplicateSubscriptionState(true)
                    .SubscribeAsync()
            
            do! producer1.DisposeAsync().AsTask()
            do! producer2.DisposeAsync().AsTask()
            
            let task = consumer.ReceiveAsync()
            
            // Check that the task doesn't fail immediately
            Expect.isFalse "" task.IsFaulted
            Expect.isFalse "" task.IsCanceled
            
            // Delete topic using HTTP request
            let deleteUrl = $"{pulsarHttpAddress}/admin/v2/persistent/{topicName}-1?force=true"
            let! (response: HttpResponseMessage) = commonHttpClient.DeleteAsync(deleteUrl)
            response.EnsureSuccessStatusCode() |> ignore
            
            do! Task.Delay(1000) // This make sure that the topic won't be recreated
            
            // Verify topic is deleted by trying to get stats (should return NotFound)
            let statsUrl = $"{pulsarHttpAddress}/admin/v2/persistent/{topicName}-1/stats"
            let! (statsResponse: HttpResponseMessage) = commonHttpClient.GetAsync(statsUrl)
            Expect.equal "" System.Net.HttpStatusCode.NotFound statsResponse.StatusCode
            
            // Check that the task is running
            Expect.isFalse "" task.IsCompleted
                
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName + "-2")
                    .CreateAsync()
                    
            do! producer.SendAsync([| 1uy |])
            
            let! (msg : Message<byte[]>) = task
            
            Expect.equal "" [| 1uy |] <| msg.GetValue()
            Log.Debug("Finished 'Delete topic subscribed by the pattern consumer should not throw error or recreate topic'")
        }

#if !NOTLS
        // Before running this test set 'maxMessageSize' for broker and 'nettyMaxFrameSizeBytes' for bookkeeper
        testTask "Send large message works fine" {

            Log.Debug("Started Send large message works fine")
            let client = getSslAdminClient()

            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("bigMessageProducer")
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("bigMessageConsumer")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let message = Array.create 10_400_000 1uy
                        let! _ = producer.SendAsync(message)
                        ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! message = consumer.ReceiveAsync()
                        if not (message.Data |> Array.forall (fun x -> x = 1uy)) then
                            failwith "incorrect message received"
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Send large message works fine")
        }
#endif
    ]
