module Pulsar.Client.IntegrationTests.Flow

open System
open Expecto
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Serilog
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    testList "flow" [

        testAsync "Send and receive 100 messages concurrently works fine with small receiver queue size" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine with small receiver queue size")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(10)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 ""
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Send and receive 100 messages concurrently works fine with small receiver queue size")
        }

        testAsync "Send and receive 100 messages concurrently works fine with small receiver queue size and no batches" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine with small receiver queue size and no batches")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(10)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 ""
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Send and receive 100 messages concurrently works fine with small receiver queue size and no batches")
        }
    ]
