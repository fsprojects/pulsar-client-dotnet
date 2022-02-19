module Pulsar.Client.IntegrationTests.Transaction

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
open FSharp.UMX

[<Tests>]
let tests =
    
    let produceTest isBatchEnabled =
        task {
            Log.Debug("Started Produce 10 messages within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnProduce"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(isBatchEnabled)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync()
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages name
                    }:> Task)

            let! txn =
                client.NewTransaction().BuildAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithTxn producer txn numberOfMessages name
                    }:> Task)

            do! producerTask
            do! Task.Delay 150
            Expect.isFalse "" consumerTask.IsCompleted

            do! txn.Commit()

            do! Async.Sleep 150
            Expect.isTrue "" consumerTask.IsCompletedSuccessfully

            Log.Debug("Finished Produce 10 messages within txn works fine")
        }
        
    let consumeTest isBatchEnabled =
        task {
            Log.Debug("Started Consume 10 messages within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnConsume"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(isBatchEnabled)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync()
                    
            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
            
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer numberOfMessages name
                    }:> Task)
            
            let! txn =
                client.NewTransaction().BuildAsync()
            
            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithTxn consumer1 txn numberOfMessages name
                    }:> Task)            
            
            do! Task.WhenAll [| producerTask; consumer1Task |] 
            
            do! txn.Abort() 
            do! consumer1.DisposeAsync().AsTask() 
            
            let! txn =
                client.NewTransaction().BuildAsync()
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "2")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
                    
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithTxn consumer2 txn numberOfMessages name
                    }:> Task)
            
            do! consumer2Task
            do! txn.Commit()
            do! consumer2.DisposeAsync().AsTask()
            
            let! consumer3 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "3")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
            use cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150.0))
            try
                let! _ = consumer3.ReceiveAsync(cts.Token)
                failwith "Unexpected success"
            with Flatten ex ->
                match ex with
                | :? TaskCanceledException -> ()
                | _ -> reraize ex
            
            Log.Debug("Finished Consume 10 messages within txn works fine")
        }
        
    let consumeCumulativeTest isBatchEnabled =
        task {
            Log.Debug("Started Consume cumulative 10 messages within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnConsume"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(isBatchEnabled)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(150.0))
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync()
                    
            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
            
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer numberOfMessages name
                    }:> Task)
            
            let! txn1 =
                client.NewTransaction().BuildAsync()
            
            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        let mutable msgId = MessageId.Earliest
                        for _ in 1..numberOfMessages do
                            let! message = consumer1.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumer1.Name, received)
                            msgId <- message.MessageId
                        do! consumer1.AcknowledgeCumulativeAsync(msgId, txn1)
                        do! Task.Delay(100)
                    }:> Task)
            
            do! Task.WhenAll [| producerTask; consumer1Task |] 
            
            do! txn1.Abort() 
            do! consumer1.DisposeAsync().AsTask() 
            
            let! txn2 =
                client.NewTransaction().BuildAsync()
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "2")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
                    
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        let mutable msgId = MessageId.Earliest
                        for _ in 1..numberOfMessages do
                            let! message = consumer2.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumer2.Name, received)
                            msgId <- message.MessageId
                        do! consumer2.AcknowledgeCumulativeAsync(msgId, txn2)
                        do! Task.Delay(100)
                    }:> Task)
            
            do! consumer2Task
            do! txn2.Commit()
            do! consumer2.DisposeAsync().AsTask()
            
            let! consumer3 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "3")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
            use cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150.0))
            try
                let! _ = consumer3.ReceiveAsync(cts.Token)
                failwith "Unexpected success"
            with Flatten ex ->
                match ex with
                | :? TaskCanceledException -> ()
                | _ -> reraize ex
            
            Log.Debug("Finished Consume cumulative 10 messages within txn works fine")
        }
        

    testList "Transaction" [
        
        testTask "Produce 10 messages within txn with batch works fine" {
            do! produceTest true 
        }
        testTask "Produce 10 messages within txn without batch works fine" {
            do! produceTest false 
        }
        
        testTask "Consume 10 messages within txn with batch works fine" {
            do! consumeTest true 
        }
        
        testTask "Consume 10 messages within txn without batch works fine" {
            do! consumeTest false 
        }
        
        testTask "Consume cumulative 10 messages within txn with batch works fine" {
            do! consumeCumulativeTest true 
        }
        
        testTask "Consume cumulative 10 messages within txn without batch works fine" {
            do! consumeCumulativeTest false 
        }
        
        testTask "Consume and Produce within txn works fine" {

            Log.Debug("Started Consume and Produce within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnConsume"
            
            let! regularProducer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("regular")
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() 
                    
            let! regularConsumer =
                 client.NewConsumer()
                    .Topic(topicName + "1")
                    .ConsumerName("regular")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName + "1")
                    .ProducerName(name)
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() 
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
            
            let regularProducerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages regularProducer numberOfMessages "regular"
                    }:> Task)
            
            let! txn =
                client.NewTransaction().BuildAsync()
                
            
            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithTxn consumer txn numberOfMessages name
                    }:> Task)
                
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithTxn producer txn numberOfMessages name
                    }:> Task)
                
            let regularConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages regularConsumer numberOfMessages "regular"
                    }:> Task)
            
            do! Task.WhenAll [| regularProducerTask; producerTask; consumerTask |] 
            do! Task.Delay 150
            Expect.isFalse "" regularConsumerTask.IsCompleted
            do! txn.Commit() 
           
            do! Task.Delay 150
            Expect.isTrue "" consumerTask.IsCompletedSuccessfully
            
            Log.Debug("Finished Consume and Produce within txn works fine")
        }
        
        testTask "Concurrent transactions works fine" {

            Log.Debug("Started Concurrent transactions works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "txnConcurrent"
            
            let! (producer1 : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name+ "1")
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() 
            
            let! (producer2 : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name + "2")
                    .EnableBatching(true)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() 
                    
            let! (consumer1 : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .ConsumerName(name + "1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
                    
            let! (consumer2 : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name + "2")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
            
            let! txn1 = client.NewTransaction().BuildAsync() 
            let! txn2 = client.NewTransaction().BuildAsync() 
            let! txn3 = client.NewTransaction().BuildAsync() 
            
            for i in 1..20 do
                do! producer1.SendAndForgetAsync(producer1.NewMessage($"TXN1.1-{i}", txn = txn1)) 
            for i in 1..20 do
                do! producer1.SendAndForgetAsync(producer1.NewMessage($"TXN2.1-{i}", txn = txn2)) 
            for i in 1..20 do
                do! producer2.SendAndForgetAsync(producer2.NewMessage($"TXN1.2-{i}", txn = txn1)) 
            for i in 1..20 do
                do! producer2.SendAndForgetAsync(producer2.NewMessage($"TXN2.2-{i}", txn = txn2)) 
            
            let cts = new CancellationTokenSource(1000)
            let getNextMessage() =
                task{
                    let t1 = consumer1.ReceiveAsync(cts.Token)
                    let t2 = consumer2.ReceiveAsync(cts.Token)
                    let! messageTask = Task.WhenAny(t1, t2)
                    let! message = messageTask
                    if t1.IsCompletedSuccessfully then
                        do! consumer1.AcknowledgeAsync(message.MessageId, txn3)
                    elif t2.IsCompletedSuccessfully then
                        do! consumer2.AcknowledgeAsync(message.MessageId, txn3)
                    return message
                }
                
            let receiveTask = getNextMessage()
            do! Task.Delay 150
            Expect.isFalse "" receiveTask.IsCompleted
            
            do! txn1.Commit() 
            let! (message1 : Message<string>) = receiveTask 
            Expect.stringStarts "" "TXN1" (message1.GetValue())            
            
            do! Task.WhenAll [| txn2.Abort() ; txn3.Commit() |] 
            
            do! consumer1.DisposeAsync().AsTask() 
            do! consumer2.DisposeAsync().AsTask() 
            
            let! (consumer3 : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name + "3")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
                    
            for i in 1..39 do
                let! (message : Message<string>) = consumer3.ReceiveAsync() 
                Expect.stringStarts "" "TXN1" (message.GetValue())
                do! consumer3.AcknowledgeAsync(message.MessageId) 
                
            let cts = new CancellationTokenSource(200)
            try
                let! (msg : Message<string>) = consumer3.ReceiveAsync(cts.Token) 
                failwith $"Unexpected success {msg.GetValue()}"
            with Flatten ex ->
                match ex with
                | :? TaskCanceledException -> ()
                | _ -> reraize ex
                
            do! Task.Delay 110
            
            Log.Debug("Finished Concurrent transactions works fine")
        }
    ]