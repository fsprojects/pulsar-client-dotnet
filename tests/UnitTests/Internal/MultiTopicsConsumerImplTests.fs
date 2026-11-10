module Pulsar.Client.UnitTests.Internal.MultiTopicsConsumerImplTests

open System
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal.LocalBroker
open Pulsar.Client.UnitTests.Internal.ProducerImplTests

let private clientConfig =
    { PulsarClientConfiguration.Default with
        StatsInterval = TimeSpan.FromMinutes(1.0)
        KeepAliveInterval = TimeSpan.FromMinutes(1.0) }

let private topics =
    [| TopicName("public/default/multi-topic-1"); TopicName("public/default/multi-topic-2") |]

let private consumerConfig =
    { ConsumerConfiguration<byte[]>.Default with
        Topics = topics
        SubscriptionName = %"test-subscription"
        AutoUpdatePartitions = false }

/// A multi-topic consumer with one child for each topic, all connected to the broker.
let private createConsumer (broker: LocalBroker) (clientConfig: PulsarClientConfiguration) (connectionPool: ConnectionPool) =
    let initInfos =
        topics
        |> Array.map (fun topic ->
            { TopicName = topic; Schema = Schema.BYTES(); SchemaProvider = None; Metadata = { Partitions = 0 } })
    MultiTopicsConsumerImpl.InitMultiTopic(consumerConfig, clientConfig, connectionPool, initInfos, lookupFor broker,
                                           ConsumerInterceptors<byte[]>.Empty, ignore).WaitAsync(testTimeout)

[<Tests>]
let tests =
    testList "MultiTopicsConsumerImpl" [
        testTask "Closing fails the pending receives without waiting for the child consumers to close" {
            let broker = new LocalBroker()
            let connectionPool = ConnectionPool(clientConfig)
            try
                let! (multiConsumer: MultiTopicsConsumerImpl<byte[]>) = createConsumer broker clientConfig connectionPool
                let consumer = multiConsumer :> IConsumer<byte[]>
                let pendingReceive = consumer.ReceiveAsync()
                let pendingBatchReceive = consumer.BatchReceiveAsync()

                // The broker doesn't answer the children, so they stay in their close until the operation timeout,
                // which is longer than this test waits.
                let closing = consumer.DisposeAsync().AsTask()
                do! broker.CloseRequested.WaitAsync(testTimeout)

                let! _ = expectFailure<AlreadyClosedException> pendingReceive
                let! _ = expectFailure<AlreadyClosedException> pendingBatchReceive
                // as in the Java client, a closing consumer rejects new requests instead of queueing them behind its close
                let! _ = expectFailure<AlreadyClosedException> (consumer.ReceiveAsync())
                let! _ = expectFailure<AlreadyClosedException> (consumer.AcknowledgeAsync(MessageId.Earliest))
                Expect.isFalse closing.IsCompleted "The close still waits for the child consumers"

                broker.ReleaseCloses()
                do! closing.WaitAsync(testTimeout)
            finally
                // also ends the children's closes when an assertion above fails
                broker.ReleaseCloses()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A request fails when a child doesn't answer it, and the consumer still closes" {
            // the connections keep their default timeouts, so no request timeout ends the broker's silence
            let consumerClientConfig = { clientConfig with OperationTimeout = TimeSpan.FromMilliseconds(250.0) }
            let seekReceived = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let broker =
                new LocalBroker(fun command _ ->
                    if command.``type`` = BaseCommand.Type.Seek then
                        seekReceived.TrySetResult() |> ignore)
            broker.ReleaseCloses()
            let connectionPool = ConnectionPool(clientConfig)
            try
                let! (multiConsumer: MultiTopicsConsumerImpl<byte[]>) = createConsumer broker consumerClientConfig connectionPool
                let consumer = multiConsumer :> IConsumer<byte[]>

                // One child's mailbox waits for the broker's answer to a seek, like a mailbox that waits for something
                // that doesn't end. The seek fails when the connections are closed at the end.
                let seeking: Task<unit> = multiConsumer.Consumers[0].SeekAsync(MessageId.Earliest)
                seeking.ContinueWith(fun (seek: Task<unit>) -> seek.Exception |> ignore) |> ignore
                do! seekReceived.Task.WaitAsync(testTimeout)

                // As in the Java client, where the child's request would time out. The consumer's mailbox waits for
                // its children here, so without an end to that it could not get to any other request, or to the close.
                let! _ = expectFailure<Pulsar.Client.Api.TimeoutException> (consumer.RedeliverUnacknowledgedMessagesAsync())

                // the child that still waits is closed without its mailbox
                do! consumer.DisposeAsync().AsTask().WaitAsync(testTimeout)
            finally
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }
    ]
