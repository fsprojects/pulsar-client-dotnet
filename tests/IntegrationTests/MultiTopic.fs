module Pulsar.Client.IntegrationTests.MultiTopic

open System
open System.Text
open Expecto
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    ftestList "basic" [
        
        testAsync "Two producers and one multiconsumer work fine" {

            Log.Debug("Started Two producers and one multiconsumer work fine")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "MultiConsumer"

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

            Log.Debug("Finished Two producers and one multiconsumer work fine")

        }
        
        // will fail if run more often than brokerDeleteInactiveTopicsFrequencySeconds (60 sec)
        testAsync "Two producers and one pattern consumer work fine" {

            Log.Debug("Started Two producers and one pattern consumer work fine")
            let client = getClient()
            let topicName1 = "public/default/topic-mypattern-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-mypattern-" + Guid.NewGuid().ToString("N")
            let name = "MultiConsumer"

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
                    .TopicsPattern("public/default/topic-mypattern-*")
                    .SubscriptionName("test-subscription")
                    .ConsumerName(name)
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
            
            do! producer1.DisposeAsync().AsTask() |> Async.AwaitTask
            do! producer2.DisposeAsync().AsTask() |> Async.AwaitTask
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask

            Log.Debug("Finished Two producers and one multiconsumer work fine")

        }
        
        ptestAsync "Eternal loop to test cluster modifications removal/addition" {

            Log.Debug("Started Eternal loop to test cluster modifications removal/addition")
            let client = getClient()

            let! producer =
                client.NewProducer()
                    .Topic("public/default/topic-loop-1")
                    .ProducerName("looping")
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.FromSeconds(6.0))
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .TopicsPattern("public/default/topic-loop-*")
                    .ConsumerName("looping")
                    .SubscriptionName("test-subscription")
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(10.0))
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        while true do
                            do! Task.Delay(5000)    
                            let time = DateTime.Now.ToShortTimeString()
                            let! msgId = producer.SendAsync(time |> Encoding.UTF8.GetBytes)
                            Log.Logger.Information("{0} sent: {1}", msgId, time)
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        try
                            while true do
                                let! msg = consumer.ReceiveAsync()
                                Log.Logger.Information("{0} received: {1}", msg.MessageId, msg.Data |> Encoding.UTF8.GetString)
                                do! consumer.AcknowledgeAsync(msg.MessageId)
                                Log.Logger.Information("{0} acknowledged: {1}", msg.MessageId, msg.Data |> Encoding.UTF8.GetString)
                        with ex ->
                            Log.Logger.Error(ex, "Receive failed")
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Eternal loop to test cluster modifications removal/addition")
        }
]
