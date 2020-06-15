module Pulsar.Client.IntegrationTests.Failover

open System
open Expecto
open Expecto.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    testList "Failover consumer test list" [ 

        testAsync "Failover consumer works fine" {

            Log.Debug("Started Failover consumer works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "failoverProducer"
            let consumerName1 = "failoverConsumer1" //master
            let consumerName2 = "failoverConsumer2" //slave
            let numberOfMessages = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName1)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() |> Async.AwaitTask
                    
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName2)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer1 numberOfMessages consumerName1
                        do! Task.Delay(100)
                        do! consumer1.DisposeAsync()
                    }:> Task)
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer2 numberOfMessages consumerName2
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) |> Async.AwaitTask

            Log.Debug("Finished Failover consumer works fine")
        }

        testAsync "Failover consumer with PriorityLevel works fine" {

            Log.Debug("Started Failover consumer with PriorityLevel works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "failoverProducer"
            let consumerName1 = "failoverConsumer1" //slave
            let consumerName2 = "failoverConsumer2" //master
            let consumerPriority1 = 1 //slave
            let consumerPriority2 = 0 //master
            let numberOfMessages = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName1)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .PriorityLevel(consumerPriority1)
                    .SubscribeAsync() |> Async.AwaitTask
                    
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName2)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .PriorityLevel(consumerPriority2)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer2 numberOfMessages consumerName2
                        do! Task.Delay(100)
                        do! consumer2.DisposeAsync()
                    }:> Task)
                
            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer1 numberOfMessages consumerName1
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) |> Async.AwaitTask

            Log.Debug("Finished Failover consumer with PriorityLevel works fine")
        }
    ]