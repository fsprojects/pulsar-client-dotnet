module Pulsar.Client.IntegrationTests.Flow

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open System.Collections.Generic
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

[<Tests>]
let tests =

    testList "flow" [

        testAsync "Send and receive 100 messages concurrently works fine with small receiver queue size" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine with small receiver queue size")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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
                ProducerBuilder(client)
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
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
