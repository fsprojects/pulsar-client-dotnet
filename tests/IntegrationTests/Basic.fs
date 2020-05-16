module Pulsar.Client.IntegrationTests.Basic

open System
open System.Threading
open System.Diagnostics

open Expecto
open Expecto.Flip
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    testList "basic" [
        testAsync "Send and receive 100 messages concurrently works fine in default configuration" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

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

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Send and receive 100 messages concurrently works fine in default configuration")
        }

        testAsync "Send 100 messages and then receiving them works fine when retention is set on namespace" {

            Log.Debug("Started send 100 messages and then receiving them works fine when retention is set on namespace")
            let client = getClient()
            let topicName = "public/retention/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            do! produceMessages producer 100 "sequential" |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("sequential")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() |> Async.AwaitTask

            do! consumeMessages consumer 100 "sequential" |> Async.AwaitTask
            Log.Debug("Finished send 100 messages and then receiving them works fine when retention is set on namespace")
        }

        testAsync "Full roundtrip (emulate Request-Response behaviour)" {

            Log.Debug("Started Full roundtrip (emulate Request-Response behaviour)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName2)
                    .ConsumerName("consumer1")
                    .SubscriptionName("my-subscriptionx")
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName1)
                    .ConsumerName("consumer2")
                    .SubscriptionName("my-subscriptiony")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer1 =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName("producer1")
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                client.NewProducer()
                    .Topic(topicName2)
                    .ProducerName("producer2")
                    .CreateAsync() |> Async.AwaitTask

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
            do! [|t1; t2; t3|] |> Task.WhenAll |> Async.AwaitTask

            Log.Debug("Finished Full roundtrip (emulate Request-Response behaviour)")
        }

        testAsync "Concurrent send and receive work fine" {

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
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

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
                do! Task.WhenAll(resultTasks) |> Async.AwaitTask
            with
            | ex when ex.InnerException.GetType() = typeof<AlreadyClosedException> ->
                ()
            | ex ->
                failtestf "Incorrect exception type %A" (ex.GetType().FullName)
            Log.Debug("Finished Concurrent send and receive work fine")
        }

        testAsync "Consumer seek earliest redelivers all messages" {

            Log.Debug("Started Consumer seek earliest redelivers all messages")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages consumerName
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            do! consumer.SeekAsync(MessageId.Earliest) |> Async.AwaitTask
            do! consumeMessages consumer numberOfMessages consumerName |> Async.AwaitTask

            Log.Debug("Finished Consumer seek earliest redelivers all messages")
        }
        
        testAsync "Failover consumer works fine" {

            Log.Debug("Started Failover consumer works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "failoverProducer"
            let consumerName1 = "failoverConsumer1"
            let consumerName2 = "failoverConsumer2"
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName1)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() |> Async.AwaitTask
                    
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName2)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer (numberOfMessages/2) producerName
                        do! Task.Delay(800)
                        do! produceMessages producer (numberOfMessages/2) producerName
                    }:> Task)

            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer1 (numberOfMessages/2) consumerName1
                        do! Task.Delay(100)
                        do! consumer1.DisposeAsync()
                    }:> Task)
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer2 (numberOfMessages/2) consumerName2
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) |> Async.AwaitTask

            Log.Debug("Finished Failover consumer works fine")
        }

        testAsync "Client, producer and consumer can't be accessed after close" {

            Log.Debug("Started 'Client, producer and consumer can't be accessed after close'")

            let client = getNewClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing1-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing2-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer1 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer1")
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer2")
                    .CreateAsync() |> Async.AwaitTask

            do! consumer1.DisposeAsync().AsTask() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer1.ReceiveAsync().Result |> ignore) |> ignore
            do! producer1.DisposeAsync().AsTask() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAndForgetAsync([||]).Result |> ignore) |> ignore
            do! client.CloseAsync() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAndForgetAsync([||]).Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.CloseAsync().Result |> ignore) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
        }

        testAsync "Scheduled message should be delivered at requested time" {

            Log.Debug("Started 'Scheduled message should be delivered at requested time'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interval = 10000L
            let producerName = "schedule-producer"
            let consumerName = "schedule-consumer"
            let sw = Stopwatch()

            let! producer =
                client.NewProducer()
                    .Topic(topicName).EnableBatching(false)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("schedule-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let now = DateTimeOffset.UtcNow;
                        let deliverAt = now.AddMilliseconds(float interval)
                        let timestamp = Nullable(deliverAt.ToUnixTimeMilliseconds())
                        let message = Encoding.UTF8.GetBytes(sprintf "Message was sent with interval '%i' milliseconds" interval)
                        sw.Start()
                        let! _ = producer.NewMessage(message, deliverAt = timestamp) |> producer.SendAsync
                        ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! message = consumer.ReceiveAsync()
                        let received = Encoding.UTF8.GetString(message.Data)
                        Log.Debug("{0} received {1}", consumerName, received)
                        sw.Stop()
                        do! consumer.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} acknowledged {1}", consumerName, received)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            let elapsed = sw.ElapsedMilliseconds

            interval - elapsed
            |> Math.Abs
            |> (>) 4000L
            |> Expect.isTrue (sprintf "Message delivered in unexpected interval %i while should be %i" elapsed interval)

            Log.Debug("Finished 'Scheduled message should be delivered at requested time'")
        }
        
#if !NOTLS
        // Before running this test set 'maxMessageSize' for broker and 'nettyMaxFrameSizeBytes' for bookkeeper 
        testAsync "Send large message works fine" {

            Log.Debug("Started Send large message works fine")
            let client = getSslAdminClient()

            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("bigMessageProducer")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("bigMessageConsumer")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

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

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Send large message works fine")
        }
#endif
    ]
