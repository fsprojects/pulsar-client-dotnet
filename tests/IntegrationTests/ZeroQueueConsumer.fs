module Pulsar.Client.IntegrationTests.ZeroQueueConsumer

open System
open System.Threading
open Expecto
open Expecto.Flip
open Expecto.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    testList "ZeroQueueConsumer" [ 
        testAsync "ZeroQueueConsumer work fine" {
            
            Log.Debug("Started ZeroQueueConsumer work fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "ZeroQueue"
            let producerName = "Producer4ZeroQueue"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .ReceiverQueueSize(0)
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
            do! Async.Sleep(110) // wait for acks
            
            Log.Debug("Finished ZeroQueueConsumer work fine")
        }
        
        testAsync "ZeroQueueConsumer with multiconsumer work fine" {

            Log.Debug("Started ZeroQueueConsumer with multiconsumer work fine")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "MultiZeroConsumer"

            let! producer1 =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName(name + "1")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! producer2 =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topics([topicName1; topicName2])
                    .SubscriptionName("test-subscription")
                    .ConsumerName(name)
                    .ReceiverQueueSize(0)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync() |> Async.AwaitTask

            let messages1 = generateMessages 10 (name + "1")
            let messages2 = generateMessages 10 (name + "2")
            let messages = Array.append messages1 messages2

            let producer1Task =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer1 messages
                    }:> Task)
                
            let producer2Task =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer2 messages
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeAndVerifyMessages consumer name messages
                    }:> Task)

            do! Task.WhenAll(producer1Task, producer2Task, consumerTask) |> Async.AwaitTask
            do! Async.Sleep(110) // wait for acks

            Log.Debug("Finished ZeroQueueConsumer with multiconsumer work fine")

        }
        
        testAsync "ZeroConsumer redelivery works well" {
            Log.Debug("Started ZeroConsumer redelivery works well")
            
            let client = getClient()
            
            let topicName = Guid.NewGuid().ToString()
            let! producer = client.NewProducer(Schema.STRING())
                                .Topic(topicName)
                                .EnableBatching(false)
                                .CreateAsync()
                                |> Async.AwaitTask
                                
            let! consumer = client.NewConsumer(Schema.STRING())
                               .Topic(topicName)
                               .SubscriptionName("test-subscription")
                               .SubscriptionType(SubscriptionType.Exclusive)
                               .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                               .NegativeAckRedeliveryDelay(TimeSpan.FromMilliseconds(100.))
                               .ReceiverQueueSize(0)
                               .SubscribeAsync()
                               |> Async.AwaitTask
            
            let firstTick = Environment.TickCount64.ToString()
            let secondTick = Environment.TickCount64.ToString()
            let! _ = producer.SendAsync(firstTick) |> Async.AwaitTask
            let! _ = producer.SendAsync(secondTick) |> Async.AwaitTask
             
            let cts = new CancellationTokenSource(TimeSpan.FromSeconds(5.))
             
            for i in 1..5 do
                 let! msg = consumer.ReceiveAsync(cts.Token) |> Async.AwaitTask
                 Expect.equal "" (msg.GetValue()) firstTick
                 do! consumer.RedeliverUnacknowledgedMessagesAsync() |> Async.AwaitTask
                 
            let! msg = consumer.ReceiveAsync(cts.Token) |> Async.AwaitTask
            do! consumer.NegativeAcknowledge(msg.MessageId) |> Async.AwaitTask
            let! msg1 = consumer.ReceiveAsync(cts.Token) |> Async.AwaitTask
            Expect.equal "" (msg1.GetValue()) secondTick
            do! consumer.AcknowledgeAsync(msg1.MessageId) |> Async.AwaitTask
            let! msg2 = consumer.ReceiveAsync(cts.Token) |> Async.AwaitTask
            Expect.equal "" (msg2.GetValue()) firstTick
            do! consumer.AcknowledgeAsync(msg2.MessageId) |> Async.AwaitTask
            
            do! Async.Sleep(110) // wait for acks

            Log.Debug("Finished ZeroConsumer redelivery works well")
            
        }
    ]