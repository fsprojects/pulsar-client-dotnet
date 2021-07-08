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
    testList "Multitopic" [
        
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
                                try
                                    let! msgId = producer.SendAsync(time |> Encoding.UTF8.GetBytes)
                                    Log.Logger.Information("MessageId {0} sent: {1}", msgId, time)
                                with Flatten ex ->
                                    Log.Logger.Error(ex, "{0} failed: {1}", "Sending", time)
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        try
                            while true do
                                let! msg = consumer.ReceiveAsync()
                                Log.Logger.Information("MessageId {0} received: {1}", msg.MessageId, msg.Data |> Encoding.UTF8.GetString)
                                do! consumer.AcknowledgeAsync(msg.MessageId)
                                Log.Logger.Information("MessageId {0} acknowledged: {1}", msg.MessageId, msg.Data |> Encoding.UTF8.GetString)
                        with ex ->
                            Log.Logger.Error(ex, "Receive failed")
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Eternal loop to test cluster modifications removal/addition")
        }
        
        testAsync "Subscribe to new topic" {
    
            let subscriptionName = "testPulsar"
            let topicPattern = sprintf "persistent://public/default/%s-*" (Guid.NewGuid().ToString("N"))
            let topic1 = topicPattern.Replace("*", "1")
            let topic2 = topicPattern.Replace("*", "2")

            let client = getClient()

            let! producer1 =
                client.NewProducer()
                    .Topic(topic1)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer =
                client.NewConsumer()
                    .TopicsPattern(topicPattern)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(4.0))
                    .SubscriptionName(subscriptionName)
                    .SubscribeAsync() |> Async.AwaitTask

            let send1 =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let msgStr = sprintf "Message #%i Sent to %s on %s" i topic1 (DateTime.Now.ToLongTimeString())
                            let! _ = producer1.SendAsync(msgStr |> Encoding.UTF8.GetBytes)
                            ()
                    } :> Task
                )
            
            let receiveAll =
                Task.Run(fun () ->
                        task {
                            for i in [1..20] do
                                let! message = consumer.ReceiveAsync()
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        } :> Task
                    )

            do! Task.WhenAll(send1) |> Async.AwaitTask
            
            let! producer2 =
                client.NewProducer()
                    .Topic(topic2)
                    .CreateAsync() |> Async.AwaitTask

            do! Async.Sleep(5000)

            let send2 =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let msgStr = sprintf "Message #%i Sent to %s on %s" i topic2 (DateTime.Now.ToLongTimeString())
                            let! _ = producer2.SendAsync(msgStr |> Encoding.UTF8.GetBytes)
                            ()
                    } :> Task
                )
            do! Task.WhenAll(send2, receiveAll) |> Async.AwaitTask
            
            Log.Debug("Finished Subscribe to new topic")
        }
        
        ptestAsync "2К of topics" {
    
            Log.Debug("Started 2К of topics")
            let subscriptionName = "testPulsar"
            let prefix = sprintf "persistent://public/default/%s" (Guid.NewGuid().ToString("N"))
            let topicPattern = $"{prefix}-*"
            let messageNumber = 2000

            let client = getClient()
              
            let producers =
                [|
                    for i in 1..messageNumber do
                        task {
                                let! producer =
                                    client
                                        .NewProducer()
                                        .Topic($"{prefix}-{i}")
                                        .ProducerName($"multitopic-{i}")
                                        .EnableBatching(false)
                                        .CreateAsync()
                                return producer
                        } 
                |]
                
            let! _ = Task.WhenAll(producers) |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .TopicsPattern(topicPattern)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(2.0))
                    .SubscriptionName(subscriptionName)
                    .SubscribeAsync() |> Async.AwaitTask
                    
            do! Async.Sleep(30000)
            Log.Warning("Finished sleeping")
            
            let mutable i = 0
            [|
                for producer in producers do
                    task {
                            let i1 = i
                            let! p = producer
                            Log.Information("{0} before send", i1)
                            let! msgId = p.SendAsync([|2uy|])
                            Log.Information("{0} after send", i1)
                            return ()
                    } :> Task
                    i <- i + 1
            |] |> Task.WaitAll
            Log.Warning("Messages sent")
            for i in 1..messageNumber do
                let! message = consumer.ReceiveAsync() |> Async.AwaitTask
                Log.Warning($"{i} I've got message")
                do! consumer.AcknowledgeAsync(message.MessageId) |> Async.AwaitTask
            
            Log.Debug("Finished 2К of topics")
        }
        
       
    ]
