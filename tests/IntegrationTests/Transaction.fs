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
            
            do! Task.WhenAll [| producerTask; consumer1Task |] |> Async.AwaitTask
            
            do! txn.Abort() |> Async.AwaitTask
            do! consumer1.DisposeAsync().AsTask() |> Async.AwaitTask
            
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
            
            do! Task.WhenAll [| producerTask; consumer1Task |] |> Async.AwaitTask
            
            do! txn1.Abort() |> Async.AwaitTask
            do! consumer1.DisposeAsync().AsTask() |> Async.AwaitTask
            
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
        
        testAsync "Produce 10 messages within txn with batch works fine" {
            do! produceTest true |> Async.AwaitTask
        }
        testAsync "Produce 10 messages within txn without batch works fine" {
            do! produceTest false |> Async.AwaitTask
        }
        
        testAsync "Consume 10 messages within txn with batch works fine" {
            do! consumeTest true |> Async.AwaitTask
        }
        
        testAsync "Consume 10 messages within txn without batch works fine" {
            do! consumeTest false |> Async.AwaitTask
        }
        
        testAsync "Consume cumulative 10 messages within txn with batch works fine" {
            do! consumeCumulativeTest true |> Async.AwaitTask
        }
        
        testAsync "Consume cumulative 10 messages within txn without batch works fine" {
            do! consumeCumulativeTest false |> Async.AwaitTask
        }
        
        testAsync "Consume and Produce within txn works fine" {

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
                    .CreateAsync() |> Async.AwaitTask
                    
            let! regularConsumer =
                 client.NewConsumer()
                    .Topic(topicName + "1")
                    .ConsumerName("regular")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName + "1")
                    .ProducerName(name)
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            let regularProducerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages regularProducer numberOfMessages "regular"
                    }:> Task)
            
            let! txn =
                client.NewTransaction().BuildAsync()
                |> Async.AwaitTask
            
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
            
            do! Task.WhenAll [| regularProducerTask; producerTask; consumerTask |] |> Async.AwaitTask
            do! Async.Sleep 150
            Expect.isFalse "" regularConsumerTask.IsCompleted
            do! txn.Commit() |> Async.AwaitTask
           
            do! Async.Sleep 150
            Expect.isTrue "" consumerTask.IsCompletedSuccessfully
            
            Log.Debug("Finished Consume and Produce within txn works fine")
        }
        
        testAsync "Concurrent transactions works fine" {

            Log.Debug("Started Concurrent transactions works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "txnConcurrent"
            
            let! producer1 =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name+ "1")
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() |> Async.AwaitTask
            
            let! producer2 =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name + "2")
                    .EnableBatching(true)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer1 =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .ConsumerName(name + "1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
                    
            let! consumer2 =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name + "2")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            let! txn1 = client.NewTransaction().BuildAsync() |> Async.AwaitTask
            let! txn2 = client.NewTransaction().BuildAsync() |> Async.AwaitTask
            let! txn3 = client.NewTransaction().BuildAsync() |> Async.AwaitTask
            
            for i in 1..20 do
                do! producer1.SendAndForgetAsync(producer1.NewMessage($"TXN1.1-{i}", txn = txn1)) |> Async.AwaitTask
            for i in 1..20 do
                do! producer1.SendAndForgetAsync(producer1.NewMessage($"TXN2.1-{i}", txn = txn2)) |> Async.AwaitTask
            for i in 1..20 do
                do! producer2.SendAndForgetAsync(producer2.NewMessage($"TXN1.2-{i}", txn = txn1)) |> Async.AwaitTask
            for i in 1..20 do
                do! producer2.SendAndForgetAsync(producer2.NewMessage($"TXN2.2-{i}", txn = txn2)) |> Async.AwaitTask
            
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
            do! Async.Sleep 150
            Expect.isFalse "" receiveTask.IsCompleted
            
            do! txn1.Commit() |> Async.AwaitTask
            let! message1 = receiveTask |> Async.AwaitTask
            Expect.stringStarts "" "TXN1" (message1.GetValue())            
            
            do! Task.WhenAll [| txn2.Abort() ; txn3.Commit() |] |> Async.AwaitTask |> Async.Ignore
            
            do! consumer1.DisposeAsync().AsTask() |> Async.AwaitTask
            do! consumer2.DisposeAsync().AsTask() |> Async.AwaitTask
            
            let! consumer3 =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name + "3")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
                    
            for i in 1..39 do
                let! message = consumer3.ReceiveAsync() |> Async.AwaitTask
                Expect.stringStarts "" "TXN1" (message.GetValue())
                do! consumer3.AcknowledgeAsync(message.MessageId) |> Async.AwaitTask
                
            let cts = new CancellationTokenSource(200)
            try
                let! msg = consumer3.ReceiveAsync(cts.Token) |> Async.AwaitTask
                failwith $"Unexpected success {msg.GetValue()}"
            with Flatten ex ->
                match ex with
                | :? TaskCanceledException -> ()
                | _ -> reraize ex
                
            do! Async.Sleep 110
            
            Log.Debug("Finished Concurrent transactions works fine")
        }
    ]