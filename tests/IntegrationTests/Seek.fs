module Pulsar.Client.IntegrationTests.Seek

open System
open System.Threading
open System.Diagnostics

open Expecto
open Expecto.Flip

open System.Text
open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    let testRandomSeek (enableBatching: bool) =
        task {
            Log.Debug("Started Seek randomly works, batching {0}", enableBatching)
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekRandomProducer"
            let consumerName = "seekRandomConsumer"
            let numberOfMessages = 10
            let numberOfRandomSeeks = 10
            let producedMessageIds = Array.zeroCreate<MessageId> numberOfMessages;

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(enableBatching)
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
        
            for i in 0..(numberOfMessages-1) do
                let! messageId = producer.SendAsync([| byte i |])
                producedMessageIds.[i] <- messageId
                
            let rand = Random()
            for _ = 1 to numberOfRandomSeeks do
                let index = rand.Next(0, numberOfMessages - 1)
                let messageId = producedMessageIds.[index]
                Log.Debug("Resetting to index {0}, msgId {1}", index, messageId)
                do! consumer.SeekAsync(messageId)
                let! message = consumer.ReceiveAsync()
                Expect.equal "" (byte (index+1)) message.Data.[0]
                
   
            Log.Debug("Finished Seek randomly works, batching {0}", enableBatching)
        }
    
    testList "Seek" [
        
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
                    .EnableBatching(false)
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
        
        testAsync "Consumer seek can be done to serialized message" {

            Log.Debug("Started Consumer seek can be done to serialized message")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"

            let! producer =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            let! msgId1 = producer.SendAsync("Hello1") |> Async.AwaitTask
            let! msgId2 = producer.SendAsync("Hello2") |> Async.AwaitTask
            let serializedMsgId = msgId1.ToByteArray()
            let deserializedMsgId = MessageId.FromByteArray(serializedMsgId)

            do! consumer.SeekAsync(deserializedMsgId) |> Async.AwaitTask
            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            
            Expect.equal "" "Hello2" <| msg.GetValue()
            Log.Debug("Finished Consumer seek can be done to serialized message")
        }
        
        testAsync "Seek in the middle of the batch works properly" {

            Log.Debug("Started Seek in the middle of the batch works properly")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .StartMessageIdInclusive()
                    .SubscribeAsync() |> Async.AwaitTask

            do! fastProduceMessages producer numberOfMessages producerName |> Async.AwaitTask
            let! message1 = consumer.ReceiveAsync() |> Async.AwaitTask
            let! message2 = consumer.ReceiveAsync() |> Async.AwaitTask
            let! message3 = consumer.ReceiveAsync() |> Async.AwaitTask
            do!
                [|
                  consumer.AcknowledgeAsync(message1.MessageId)
                  consumer.AcknowledgeAsync(message2.MessageId)
                  consumer.AcknowledgeAsync(message3.MessageId)
                |]
                |> Task.WhenAll |> Async.AwaitTask |> Async.Ignore
            do! Async.Sleep 110
            do! consumer.SeekAsync(message2.MessageId) |> Async.AwaitTask
            let! message2x = consumer.ReceiveAsync() |> Async.AwaitTask
            let! message3x = consumer.ReceiveAsync() |> Async.AwaitTask
            do!
                [|
                  consumer.AcknowledgeAsync(message2x.MessageId)
                  consumer.AcknowledgeAsync(message3x.MessageId)
                |] |> Task.WhenAll |> Async.AwaitTask |> Async.Ignore  
             
            Expect.equal "" message2.MessageId message2x.MessageId
            Expect.equal "" message3.MessageId message3x.MessageId
            
            Log.Debug("Finished Seek in the middle of the batch works properly")
        }
        
        testAsync "Seek in the middle of the batch works properly 2" {

            Log.Debug("Started Seek in the middle of the batch works properly 2")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
        
            let tasks =
                [|
                    producer.SendAsync(Encoding.UTF8.GetBytes("1"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("2"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("3"))
                |]
                
            let! msgIds = tasks |> Task.WhenAll |> Async.AwaitTask
            
            do! consumer.SeekAsync(msgIds.[1]) |> Async.AwaitTask
            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            
            Expect.equal "" "3" (msg.GetValue() |> Encoding.UTF8.GetString )
   
            Log.Debug("Finished Seek in the middle of the batch works properly 2")
        }
        
        testAsync "Seek randomly works with batching " {
            do! testRandomSeek true |> Async.AwaitTask
        }
        
        testAsync "Seek randomly works without batching " {
            do! testRandomSeek true |> Async.AwaitTask
        }
       
    ]
