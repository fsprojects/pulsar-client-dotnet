module Pulsar.Client.IntegrationTests.Cancellation

open System
open System.Collections
open System.Collections.Generic
open System.Threading
open Expecto
open Expecto.Flip

open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    testList "Cancellation" [

        testTask "Cancellation without receiving a single message works fine" {

            Log.Debug("Started Cancellation without receiving a single message works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts = new CancellationTokenSource()
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let consumerTask = consumer.ReceiveAsync(cts.Token)
            cts.CancelAfter 100
            try
                do! consumerTask 
                failwith "Unexpected success"
            with _ ->
                Expect.equal "" consumerTask.Status TaskStatus.Canceled
                
            Log.Debug("Finished Cancellation without receiving a single message works fine")
        }
        
        testTask "Cancellation without receiving a single message works fine (multitopic)" {

            Log.Debug("Started Cancellation without receiving a single message works fine (multitopic)")
            let client = getClient()            
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts = new CancellationTokenSource()
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topics(seq{topicName1;topicName2})
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let consumerTask = consumer.ReceiveAsync(cts.Token)
            cts.CancelAfter 100
            try
                do! consumerTask 
                failwith "Unexpected success"
            with _ ->
                Expect.equal "" consumerTask.Status TaskStatus.Canceled
                
            Log.Debug("Finished Cancellation without receiving a single message works fine (multitopic)")
        }        
        
        testTask "Cancellation of first of two waiters works fine" {

            Log.Debug("Started Cancellation of first of two waiters works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts1 = new CancellationTokenSource()
            let cts2 = new CancellationTokenSource()
            
            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let task1 = consumer.ReceiveAsync(cts1.Token)
            let task2 = consumer.ReceiveAsync(cts2.Token)
            let resultingTask = Task.WhenAll(task1, task2)
            
            cts1.CancelAfter 100
            do! Task.Delay 150
            
            let text = "Hello!"
            let! _ = producer.SendAsync(text) 
            
            try
                let! _ = resultingTask 
                failwith "Unexpected success"
            with _ ->
                Expect.equal "" TaskStatus.Canceled task1.Status
                Expect.equal "" TaskStatus.RanToCompletion task2.Status
                Expect.equal "" text (task2.Result.GetValue())
                
            Log.Debug("Finished Cancellation of first of two waiters works fine")
        }
        
        testTask "Cancellation of first of two waiters works fine (multitopic)" {

            Log.Debug("Started Cancellation of first of two waiters works fine (multitopic)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts1 = new CancellationTokenSource()
            let cts2 = new CancellationTokenSource()
            
            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName1)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topics(seq { topicName1; topicName2 })
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let task1 = consumer.ReceiveAsync(cts1.Token)
            let task2 = consumer.ReceiveAsync(cts2.Token)
            let resultingTask = Task.WhenAll(task1, task2)
            
            cts1.CancelAfter 100
            do! Task.Delay 150
            
            let text = "Hello!"
            let! _ = producer.SendAsync(text) 
            
            try
                let! _ = resultingTask 
                failwith "Unexpected success"
            with _ ->
                Expect.equal "" TaskStatus.Canceled task1.Status
                Expect.equal "" TaskStatus.RanToCompletion task2.Status
                Expect.equal "" text (task2.Result.GetValue())
                
            Log.Debug("Finished Cancellation of first of two waiters works fine (multitopic)")
        }
        
        testTask "Cancellation of first of two waiters works fine with batch receive" {

            Log.Debug("Started Cancellation of first of two waiters works fine with batch receive")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts1 = new CancellationTokenSource()
            let cts2 = new CancellationTokenSource()
            
            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .BatchReceivePolicy(BatchReceivePolicy(2, -1L, TimeSpan.FromHours(1.0)))
                    .SubscribeAsync() 

            let task1 = consumer.BatchReceiveAsync(cts1.Token)
            let task2 = consumer.BatchReceiveAsync(cts2.Token)
            let resultingTask = Task.WhenAll(task1, task2)
            
            let! _ = producer.SendAsync("Hello 1") 
            cts1.CancelAfter 100
            do! Task.Delay 150
            
            let! _ = producer.SendAsync("Hello 2") 
            
            try
                let! _ = resultingTask 
                failwith "Unexpected success"
            with ex ->
                Expect.equal "" TaskStatus.Canceled task1.Status
                Expect.equal "" TaskStatus.RanToCompletion task2.Status
                Expect.sequenceEqual "" [| "Hello 1"; "Hello 2" |] (task2.Result |> Seq.map (fun m -> m.GetValue()))
                
            Log.Debug("Finished Cancellation of first of two waiters works fine with batch receive")
        }
                
        testTask "Cancellation of first of two waiters works fine with batch receive (multitopic)" {

            Log.Debug("Started Cancellation of first of two waiters works fine with batch receive (multitopic)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let cts1 = new CancellationTokenSource()
            let cts2 = new CancellationTokenSource()
            
            let! (producer1 : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName1)
                    .CreateAsync() 
                    
            let! (producer2 : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName1)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topics(seq { topicName1; topicName2 })
                    .SubscriptionName("test-subscription")
                    .BatchReceivePolicy(BatchReceivePolicy(2, -1L, TimeSpan.FromHours(1.0)))
                    .SubscribeAsync() 

            let task1 = consumer.BatchReceiveAsync(cts1.Token)
            let task2 = consumer.BatchReceiveAsync(cts2.Token)
            let resultingTask = Task.WhenAll(task1, task2)
            
            let! _ = producer1.SendAsync("Hello 1") 
            cts1.CancelAfter 100
            do! Task.Delay 150
            
            let! _ = producer2.SendAsync("Hello 2") 
            
            try
                let! _ = resultingTask 
                failwith "Unexpected success"
            with _ ->
                Expect.equal "" TaskStatus.Canceled task1.Status
                Expect.equal "" TaskStatus.RanToCompletion task2.Status
                Expect.sequenceEqual "" [| "Hello 1"; "Hello 2" |] (task2.Result |> Seq.map (fun m -> m.GetValue()))
                
            Log.Debug("Finished Cancellation of first of two waiters works fine with batch receive (multitopic)")
        }

    ]
