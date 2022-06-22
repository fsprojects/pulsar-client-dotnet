module Pulsar.Client.IntegrationTests.Chunks

open Pulsar.Client.Api
open Pulsar.Client.Common

#nowarn "25"

open System
open Expecto
open Expecto.Flip

open System.Threading.Tasks
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    testList "Chunks" [
        
        testTask "Two chunks-message delivered successfully" {
            Log.Debug("Started Two chunks-message delivered successfully")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "twoChunks"

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CompressionType(CompressionType.Snappy)
                    .CreateAsync() 

            let! (consumer :  IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let payload = Array.zeroCreate 10_000_000
            Random().NextBytes(payload)
            payload.[0] <- 0uy
            payload.[1] <- 1uy
            payload.[8_000_000] <- 1uy
            payload.[9_000_000] <- 0uy
            let! msgId =
                producer.NewMessage(payload)
                |> producer.SendAsync
                
                
            let! (msg : Message<byte[]>) =
                consumer.ReceiveAsync()
                
            
            Expect.equal "" msgId msg.MessageId
            Expect.equal "" 1uy msg.Data.[1] 
            Expect.equal "" 1uy msg.Data.[8_000_000] 
            Expect.equal "" 0uy msg.Data.[0] 
            Expect.equal "" 0uy msg.Data.[9_000_000] 
        
            do! consumer.UnsubscribeAsync() 
            do! Task.Delay 100
            Log.Debug("Ended Two chunks-message delivered successfully")
        }
        
        testTask "Two parallel chunks-message delivered successfully with short queue" {
            Log.Debug("Started Two parallel chunks-message delivered successfully with short queue")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "parallelChunks"

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name + "1")
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CreateAsync() 

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name + "2")
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .MaxPendingChunkedMessage(1)
                    .AckTimeout(TimeSpan.FromMilliseconds(1000.0))
                    .SubscribeAsync() 

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
                
                 
            let! ([| msg1; msg2 |] : Message<byte[]>[]) =
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
         
            do! consumer.UnsubscribeAsync() 
            do! Task.Delay 100
             
            Log.Debug("Ended Two parallel chunks-message delivered successfully with short queue")
        }

        testTask "Seek chunk messages and receive correctly" {
            Log.Debug("Started Seek chunk messages and receive correctly")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "seekChunkMessages"

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .EnableChunking(true)
                    .CompressionType(CompressionType.Snappy)
                    .CreateAsync() 

            let! (consumer :  IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .StartMessageIdInclusive()
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let payload = Array.zeroCreate 10_000_000
            Random().NextBytes(payload)
            
            let! (msgIds: MessageId[]) =
                [| for i in 0 .. 9 do
                     producer.NewMessage(payload)
                     |> producer.SendAsync|]
                |> Task.WhenAll

            for i in 0 .. 9 do
                do! consumer.ReceiveAsync()

            do! consumer.SeekAsync(msgIds.[1])
            for i in 1 .. 9 do
                let! (msgAfterSeek : Message<byte[]>) = consumer.ReceiveAsync()
                Expect.equal "" msgIds.[i] msgAfterSeek.MessageId
        
            do! consumer.UnsubscribeAsync()
            
            let! (reader :  IReader<byte[]>) =
                client.NewReader()
                    .Topic(topicName)
                    .StartMessageIdInclusive()
                    .StartMessageId(msgIds.[1])
                    .CreateAsync()
                    
            let! (readMsg : Message<byte[]>) = reader.ReadNextAsync()
            Expect.equal "" msgIds.[1] readMsg.MessageId      
            
            Log.Debug("Ended Seek chunk messages and receive correctly")
        }
    ]