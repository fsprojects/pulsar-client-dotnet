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
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open System.Collections.Generic
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

[<Tests>]
let tests =

    testList "basic" [
        testAsync "Send and receive 100 messages concurrently works fine in default configuration" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

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
                        do! produceMessages producer 100 "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 "concurrent"
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

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
                        let! _ = producer2.SendAsync(message.Payload)
                        ()
                } |> Task.WaitAll
                Log.Debug("t3 ended")
            )
            [|t1; t2; t3|] |> Task.WaitAll

            Log.Debug("Finished Full roundtrip (emulate Request-Response behaviour)")
        }

        testAsync "Send and receive 100 messages concurrently works fine with small receiver queue size" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine with small receiver queue size")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(10)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 ""
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine with small receiver queue size")
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
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAsync([||]).Result |> ignore) |> ignore
            do! client.CloseAsync() |> Async.AwaitTask
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAsync([||]).Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.GetPartitionedTopicMetadata(%"abc").Result |> ignore) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
        }
    ]
