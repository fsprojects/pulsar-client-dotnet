module Pulsar.Client.IntegrationTests.Transaction

open System
open System.Threading
open System.Diagnostics

open Expecto
open Expecto.Flip
open FSharp.Control.Tasks.V2.ContextInsensitive
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

    testList "Transaction" [
        
        testAsync "Produce 10 messages within txn works fine" {

            Log.Debug("Started Produce 10 messages within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnProduce"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
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
            
            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages name
                    }:> Task)
            
            let! txn =
                client.NewTransaction().BuildAsync()
                |> Async.AwaitTask
          
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithTxn producer txn numberOfMessages name
                    }:> Task)
            
            do! producerTask |> Async.AwaitTask
            do! Async.Sleep 150
            Expect.isFalse "" consumerTask.IsCompleted
            
            do! txn.Commit() |> Async.AwaitTask
            
            do! Async.Sleep 150
            Expect.isTrue "" consumerTask.IsCompletedSuccessfully

            Log.Debug("Finished Produce 10 messages within txn works fine")
        }
        
        testAsync "Consume 10 messages within txn works fine" {

            Log.Debug("Started Consume 10 messages within txn works fine")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnConsume"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages name
                    }:> Task)
            
            let! txn =
                client.NewTransaction().BuildAsync()
                |> Async.AwaitTask
            
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
                |> Async.AwaitTask
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "2")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
                    
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithTxn consumer2 txn numberOfMessages name
                    }:> Task)
            
            do! consumer2Task |> Async.AwaitTask
            do! txn.Commit() |> Async.AwaitTask
            do! consumer2.DisposeAsync().AsTask() |> Async.AwaitTask
            
            let! consumer3 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name + "3")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            use cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150.0))
            try
                let! _ = consumer3.ReceiveAsync(cts.Token) |> Async.AwaitTask
                failwith "Unexpected success"
            with Flatten ex ->
                match ex with
                | :? TaskCanceledException -> ()
                | _ -> reraize ex
            
            Log.Debug("Finished Consume 10 messages within txn works fine")
        }
    ]