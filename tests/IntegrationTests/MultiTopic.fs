module Pulsar.Client.IntegrationTests.MultiTopic

open System
open System.Collections.Generic
open System.Text
open Expecto

open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open Serilog
open FSharp.UMX

[<Tests>]
let tests =
    testList "Multitopic" [
        
        testTask "Two producers and one multiconsumer work fine" {

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
                    .CreateAsync() 
                    
            let! producer2 =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! consumer =
                client.NewConsumer()
                    .Topics([topicName1; topicName2])
                    .SubscriptionName("test-subscription")
                    .ConsumerName(name)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync() 

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

            do! Task.WhenAll(producer1Task, producer2Task, consumerTask) 
            do! Task.Delay(110) // wait for acks

            Log.Debug("Finished Two producers and one multiconsumer work fine")

        }
        
        // will fail if run more often than brokerDeleteInactiveTopicsFrequencySeconds (60 sec)
        testTask "Two producers and one pattern consumer work fine" {

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
                    .CreateAsync() 
                    
            let! producer2 =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! consumer =
                client.NewConsumer()
                    .TopicsPattern("public/default/topic-mypattern-*")
                    .SubscriptionName("test-subscription")
                    .ConsumerName(name)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync() 

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

            do! Task.WhenAll(producer1Task, producer2Task, consumerTask)             
            do! Task.Delay(110) // wait for acks
            
            do! producer1.DisposeAsync().AsTask() 
            do! producer2.DisposeAsync().AsTask() 
            do! consumer.UnsubscribeAsync() 

            Log.Debug("Finished Two producers and one multiconsumer work fine")

        }
        
        ptestTask "Eternal loop to test cluster modifications removal/addition" {

            Log.Debug("Started Eternal loop to test cluster modifications removal/addition")
            let client = getClient()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic("public/default/topic-loop-1")
                    .ProducerName("looping")
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.FromSeconds(6.0))
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern("public/default/topic-loop-*")
                    .ConsumerName("looping")
                    .SubscriptionName("test-subscription")
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(10.0))
                    .SubscribeAsync() 

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

            do! Task.WhenAll(producerTask, consumerTask) 

            Log.Debug("Finished Eternal loop to test cluster modifications removal/addition")
        }
        
        testTask "Subscribe to new topic" {
    
            let subscriptionName = "testPulsar"
            let topicPattern = sprintf "persistent://public/default/%s-*" (Guid.NewGuid().ToString("N"))
            let topic1 = topicPattern.Replace("*", "1")
            let topic2 = topicPattern.Replace("*", "2")

            let client = getClient()

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topic1)
                    .CreateAsync() 
                    
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(topicPattern)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(4.0))
                    .SubscriptionName(subscriptionName)
                    .SubscribeAsync() 

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

            do! Task.WhenAll(send1) 
            
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topic2)
                    .CreateAsync() 

            do! Task.Delay(5000)

            let send2 =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let msgStr = sprintf "Message #%i Sent to %s on %s" i topic2 (DateTime.Now.ToLongTimeString())
                            let! _ = producer2.SendAsync(msgStr |> Encoding.UTF8.GetBytes)
                            ()
                    } :> Task
                )
            do! Task.WhenAll(send2, receiveAll) 
            
            Log.Debug("Finished Subscribe to new topic")
        }
        
        ptestTask "2К of topics" {
    
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
                
            let! _ = Task.WhenAll(producers) 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(topicPattern)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(2.0))
                    .SubscriptionName(subscriptionName)
                    .SubscribeAsync() 
                    
            do! Task.Delay(30000)
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
                let! (message : Message<byte[]>) = consumer.ReceiveAsync() 
                Log.Warning($"{i} I've got message")
                do! consumer.AcknowledgeAsync(message.MessageId) 
            
            Log.Debug("Finished 2К of topics")
        }
        
        testTask "Multiple topic seek by function" {
            let prefix = $"persistent://public/default/topic-seektest-{Guid.NewGuid():N}-"
            let topicName1 = prefix + "1"
            let topicName2 = prefix + "2"
            let client = getClient()

            let name = "MultiConsumer"

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName(name + "1")
                    .EnableBatching(false)
                    .CreateAsync() 
                    
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName2)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .CreateAsync() 

            let messages1 = generateMessages 10 (name + "1")
            let messages2 = generateMessages 10 (name + "2")
            let ids1 = List<MessageId>()
            let ids2 = List<MessageId>()
            let times = List<TimeStamp>()
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics([ topicName1; topicName2])
                    .SubscriptionName("test-seektest-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() 
                    
            for msg in messages1 do
                let! messageIdSent = producer1.SendAsync(Encoding.UTF8.GetBytes(msg)) 
                ids1.Add(messageIdSent)
                
            for msg in messages2 do
                let! messageIdSent = producer2.SendAsync(Encoding.UTF8.GetBytes(msg))  
                DateTime.UtcNow |> extractTimeStamp |> times.Add
                ids2.Add(messageIdSent)
                do! Task.Delay(100)
            
            do! consumer.SeekAsync(fun topicName ->
                    match topicName with
                    | t when t = topicName1 -> SeekType.MessageId ids1.[3] 
                    | t when t = topicName2 -> SeekType.Timestamp times.[5]
                    | _ -> SeekType.MessageId MessageId.Latest
                )    
                
                
            let rec consumerLoop (msg1, msg2) = task {

                match msg1, msg2 with
                | Some _, Some _ -> return msg1, msg2
                | _ -> 
                    let! msg = consumer.ReceiveAsync() 
                    
                    let next =
                        match msg1, msg2 with
                        | None, _ when msg.MessageId.TopicName = %topicName1 -> Some msg, msg2
                        | _, None when msg.MessageId.TopicName = %topicName2 -> msg1, Some msg
                        | _ -> msg1, msg2
                        
                    return! consumerLoop next
                
            }
            
            let! ((msg1, msg2) : Message<byte[]> option * Message<byte[]> option) = consumerLoop (None, None)
                                                    
            do! producer1.DisposeAsync().AsTask() 
            do! producer2.DisposeAsync().AsTask() 
            do! consumer.UnsubscribeAsync() 
            
            Expect.equal msg1.IsSome true "The message from topic1 is found" 
            Expect.equal msg2.IsSome true "The message from topic2 is found"
            Expect.equal msg1.Value.MessageId ids1.[4] "Topic started from message 3"
            Expect.equal msg2.Value.MessageId ids2.[6] "Topic started from message 6"
            
            Log.Debug("Finished Multiple topic seek with resolver function")
        }
    ]
