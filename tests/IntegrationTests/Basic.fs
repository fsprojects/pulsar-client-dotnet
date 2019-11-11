module Pulsar.Client.IntegrationTests.Basic

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX
open System.Threading

[<Tests>]
let tests =

    testList "basic" [
        testAsync "Send and receive 100 messages concurrently works fine in default configuration" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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
                ProducerBuilder(client)
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            do! produceMessages producer 100 "sequential" |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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
                ConsumerBuilder(client)
                    .Topic(topicName2)
                    .ConsumerName("consumer1")
                    .SubscriptionName("my-subscriptionx")
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName1)
                    .ConsumerName("consumer2")
                    .SubscriptionName("my-subscriptiony")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer1 =
                ProducerBuilder(client)
                    .Topic(topicName1)
                    .ProducerName("producer1")
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                ProducerBuilder(client)
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
                        let received = Encoding.UTF8.GetString(message.Payload)
                        do! consumer2.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} received {1}", "consumer2", received)
                        let expected = "Message #" + string i
                        if received.StartsWith(expected) |> not then
                            failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "consumer2"
                        let! _ = producer2.SendAndForgetAsync(message.Payload)
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
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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
                            let received = Encoding.UTF8.GetString(message.Payload)
                            Log.Debug("{0}-{1} received {2}", consumerName, i, received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            Log.Debug("{0}-{1} acknowledged {2}", consumerName, i, received)
                            if Interlocked.Increment(&processedCount) = (numberOfMessages*3) then
                                do! consumer.CloseAsync()
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
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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

        testAsync "Client, producer and consumer can't be accessed after close" {

            Log.Debug("Started 'Client, producer and consumer can't be accessed after close'")

            let client = getNewClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing1-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing2-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer1 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("ClosingProducer1")
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("ClosingProducer2")
                    .CreateAsync() |> Async.AwaitTask

            do! consumer1.CloseAsync() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer1.ReceiveAsync().Result |> ignore) |> ignore
            do! producer1.CloseAsync() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAndForgetAsync([||]).Result |> ignore) |> ignore
            do! client.CloseAsync() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAndForgetAsync([||]).Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.GetPartitionedTopicMetadata(%"abc").Result |> ignore) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
        }

    ]
