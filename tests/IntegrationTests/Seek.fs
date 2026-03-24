module Pulsar.Client.IntegrationTests.Seek

open System
open System.Threading
open System.Diagnostics
open System.Collections.Generic

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
        
        testTask "Consumer seek earliest redelivers all messages" {

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
                    .CreateAsync() 

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

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

            do! Task.WhenAll(producerTask, consumerTask) 
            do! consumer.SeekAsync(MessageId.Earliest) 
            do! consumeMessages consumer numberOfMessages consumerName 

            Log.Debug("Finished Consumer seek earliest redelivers all messages")
        }
        
        testTask "Consumer seek can be done to serialized message" {

            Log.Debug("Started Consumer seek can be done to serialized message")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"

            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() 

            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
            
            let! (msgId1 : MessageId) = producer.SendAsync("Hello1") 
            let! msgId2 = producer.SendAsync("Hello2") 
            let serializedMsgId = msgId1.ToByteArray()
            let deserializedMsgId = MessageId.FromByteArray(serializedMsgId)

            do! consumer.SeekAsync(deserializedMsgId) 
            let! (msg : Message<string>) = consumer.ReceiveAsync() 
            
            Expect.equal "" "Hello2" <| msg.GetValue()
            Log.Debug("Finished Consumer seek can be done to serialized message")
        }
        
        testTask "Seek in the middle of the batch works properly" {

            Log.Debug("Started Seek in the middle of the batch works properly")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .StartMessageIdInclusive()
                    .SubscribeAsync() 

            do! fastProduceMessages producer numberOfMessages producerName 
            let! (message1 : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message2 : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message3 : Message<byte[]>) = consumer.ReceiveAsync() 
            do!
                [|
                  consumer.AcknowledgeAsync(message1.MessageId)
                  consumer.AcknowledgeAsync(message2.MessageId)
                  consumer.AcknowledgeAsync(message3.MessageId)
                |]
                |> Task.WhenAll 
            do! Task.Delay 110
            do! consumer.SeekAsync(message2.MessageId) 
            let! (message2x : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message3x : Message<byte[]>) = consumer.ReceiveAsync() 
            do!
                [|
                  consumer.AcknowledgeAsync(message2x.MessageId)
                  consumer.AcknowledgeAsync(message3x.MessageId)
                |] |> Task.WhenAll   
             
            Expect.equal "" message2.MessageId message2x.MessageId
            Expect.equal "" message3.MessageId message3x.MessageId
            
            Log.Debug("Finished Seek in the middle of the batch works properly")
        }
        
        testTask "Seek in the middle of the batch works properly 2" {

            Log.Debug("Started Seek in the middle of the batch works properly 2")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
        
            let tasks =
                [|
                    producer.SendAsync(Encoding.UTF8.GetBytes("1"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("2"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("3"))
                |]
                
            let! (msgIds : MessageId[]) = tasks |> Task.WhenAll 
            
            do! consumer.SeekAsync(msgIds.[1]) 
            let! (msg : Message<byte[]>) = consumer.ReceiveAsync() 
            
            Expect.equal "" "3" (msg.GetValue() |> Encoding.UTF8.GetString )
   
            Log.Debug("Finished Seek in the middle of the batch works properly 2")
        }
        
        testTask "Seek randomly works with batching " {
            do! testRandomSeek true 
        }
        
        testTask "Seek randomly works without batching " {
            do! testRandomSeek true 
        }
        
        
        testTask "Seek won't get stuck at the receive or receive duplicate messages in MultiTopicsConsumer" {
            Log.Debug("Started Seek won't get stuck at the receive in MultiTopicsConsumer")
            let client = getClient()
            let topicName = "persistent://public/default/multi-topic-seek"
            let producerName = "seekStuckProducer"
            let consumerName = "seekStuckConsumer"
            let numberOfMessages = 30
            let numberOfMessagesBeforeSeek = 10
            let subscriptionName = "test-seek-stuck-" + Guid.NewGuid().ToString("N")
            
            let seekWithRetry (consumer: IConsumer<byte[]>) (targetTimestamp: TimeStamp) (maxRetries: int) =
                task {
                    let mutable retryCount = 0
                    let mutable success = false
                    while retryCount < maxRetries && not success do
                        try
                            do! consumer.SeekAsync(targetTimestamp)
                            success <- true
                        with Flatten ex ->
                            match ex with
                            | :? NotConnectedException as notConnectedEx ->
                                retryCount <- retryCount + 1
                                if retryCount >= maxRetries then
                                    Log.Error("SeekAsync failed after {0} retries: {1}", maxRetries, notConnectedEx.Message)
                                    raise notConnectedEx
                                else
                                    Log.Debug("SeekAsync failed (attempt {0}/{1}): {2}. Retrying in 1 second...", retryCount, maxRetries, notConnectedEx.Message)
                                    do! Task.Delay(1000)
                            | _ ->
                                raise ex
                }
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName(subscriptionName)
                    .ReceiverQueueSize(10)
                    .SubscribeAsync()
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync()
            
            let messagesBeforeSeek = HashSet<string>()
            for i in 1..numberOfMessagesBeforeSeek do
                let messageContent = sprintf "BeforeSeek-%i-%s" i (Guid.NewGuid().ToString("N"))
                messagesBeforeSeek.Add(messageContent) |> ignore
                let messageBytes = Encoding.UTF8.GetBytes(messageContent)
                let! (_ : MessageId) = producer.SendAsync(messageBytes)
                ()
            do! Task.Delay(1000)

            let targetTimestamp = %(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())

            let expectedMessages = HashSet<string>()
            for i in 1..numberOfMessages do
                let messageContent = sprintf "AfterSeek-%i-%s" i (Guid.NewGuid().ToString("N"))
                expectedMessages.Add(messageContent) |> ignore
                let messageBytes = Encoding.UTF8.GetBytes(messageContent)
                let! (_ : MessageId) = producer.SendAsync(messageBytes)
                ()
            do! Task.Delay(1000)

            Log.Debug("Seeking to timestamp: {0}", targetTimestamp)
            do! seekWithRetry consumer targetTimestamp 10
            
            let receivedMessages = HashSet<string>()
            let cts = new CancellationTokenSource(TimeSpan.FromSeconds(30.0))
            
            try
                for _ in 1..numberOfMessages do
                    let! (message : Message<byte[]>) = consumer.ReceiveAsync(cts.Token)
                    let received = Encoding.UTF8.GetString(message.Data)
                    Log.Debug("{0} received {1}", consumerName, received)
                    receivedMessages.Add(received) |> ignore
                    do! consumer.AcknowledgeAsync(message.MessageId)
                
                Expect.equal $"Expected to receive {numberOfMessages} messages, but got {receivedMessages.Count}" numberOfMessages receivedMessages.Count
                for expectedMsg in expectedMessages do
                    Expect.isTrue $"Missing expected message: {expectedMsg}" (receivedMessages.Contains(expectedMsg))
                for oldMsg in messagesBeforeSeek do
                    Expect.isFalse $"Received stale pre-seek message: {oldMsg}" (receivedMessages.Contains(oldMsg))

                let noMoreMessagesCts = new CancellationTokenSource(TimeSpan.FromSeconds(5.0))
                try
                    try
                        let! (extraMessage : Message<byte[]>) = consumer.ReceiveAsync(noMoreMessagesCts.Token)
                        let extraReceived = Encoding.UTF8.GetString(extraMessage.Data)
                        let errorMsg = $"Unexpected extra message received within 5 seconds: {extraReceived}"
                        Log.Error(errorMsg)
                        failwith errorMsg
                    with
                    | :? OperationCanceledException
                    | :? TaskCanceledException ->
                        ()
                finally
                    noMoreMessagesCts.Dispose()
                
                cts.Dispose()
            with
            | :? OperationCanceledException
            | :? TaskCanceledException ->
                cts.Dispose()
                let errorMsg = $"Test timeout: Only received {receivedMessages.Count} out of {numberOfMessages} messages within 30 seconds"
                Log.Error(errorMsg)
                failwith errorMsg
            | ex ->
                cts.Dispose()
                raise ex
            
            Log.Debug("Finished Seek won't get stuck at the receive in MultiTopicsConsumer")
        }
       
    ]
