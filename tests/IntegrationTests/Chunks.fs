module Pulsar.Client.IntegrationTests.Chunks

open Pulsar.Client.Common

#nowarn "25"

open System
open Expecto
open Expecto.Flip
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    testList "basic" [
        
        testAsync "Two chunks-message delivered successfully" {
            Log.Debug("Started Two chunks-message delivered successfully")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "twoChunks"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CompressionType(CompressionType.Snappy)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let payload = Array.zeroCreate 10_000_000
            Random().NextBytes(payload)
            payload.[0] <- 0uy
            payload.[1] <- 1uy
            payload.[8_000_000] <- 1uy
            payload.[9_000_000] <- 0uy
            let! msgId =
                producer.NewMessage(payload)
                |> producer.SendAsync
                |> Async.AwaitTask
                
            let! msg =
                consumer.ReceiveAsync()
                |> Async.AwaitTask
            
            Expect.equal "" msgId msg.MessageId
            Expect.equal "" 1uy msg.Data.[1] 
            Expect.equal "" 1uy msg.Data.[8_000_000] 
            Expect.equal "" 0uy msg.Data.[0] 
            Expect.equal "" 0uy msg.Data.[9_000_000] 
        
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
            do! Async.Sleep 100
            Log.Debug("Ended Two chunks-message delivered successfully")
        }
        
        testAsync "Two parallel chunks-message delivered successfully with short queue" {
            Log.Debug("Started Two parallel chunks-message delivered successfully with short queue")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "parallelChunks"

            let! producer1 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name + "1")
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .MaxPendingChunkedMessage(1)
                    .AckTimeout(TimeSpan.FromMilliseconds(1000.0))
                    .SubscribeAsync() |> Async.AwaitTask

            let payload1 = Array.zeroCreate 10_000_000
            let payload2 = Array.zeroCreate 10_000_000
            payload1.[1] <- 1uy
            payload1.[8_000_000] <- 1uy
            payload2.[0] <- 1uy
            payload2.[9_000_000] <- 1uy
            let! [| msgId1; msgId2 |] =
                [| producer1.NewMessage(payload1)|> producer1.SendAsync
                   producer2.NewMessage(payload2)|> producer2.SendAsync |]
                |> Task.WhenAll
                |> Async.AwaitTask
                 
            let! [| msg1; msg2 |] =
                [| task {
                       let! msg = consumer.ReceiveAsync()
                       do! consumer.AcknowledgeAsync(msg.MessageId)
                       return msg
                   }
                   task {
                        let! msg = consumer.ReceiveAsync()
                        do! consumer.AcknowledgeAsync(msg.MessageId)
                        return msg
                   } |]
                |> Task.WhenAll
                |> Async.AwaitTask
            
            let [ one; two ] = 
                if msg1.Data.[0] = 0uy then
                    [ msg1; msg2 ]
                else
                    [ msg2; msg1 ]
            Expect.equal "" msgId1 one.MessageId
            Expect.equal "" msgId2 two.MessageId
            Expect.equal "" 1uy one.Data.[1]
            Expect.equal "" 1uy one.Data.[8_000_000]
            Expect.equal "" 0uy one.Data.[0]
            Expect.equal "" 0uy one.Data.[9_000_000]
            Expect.equal "" 0uy two.Data.[1]
            Expect.equal "" 0uy two.Data.[8_000_000]
            Expect.equal "" 1uy two.Data.[0]
            Expect.equal "" 1uy two.Data.[9_000_000]
         
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
            do! Async.Sleep 100
             
            Log.Debug("Ended Two parallel chunks-message delivered successfully with short queue")
        }
    ]