module Pulsar.Client.IntegrationTests.Batching

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


    testList "basic" [

        testAsync "Batch get sended if batch size exceeds" {

            Log.Debug("Started 'Batch get sended if batch size exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync() |> Async.AwaitTask

            do! fastProduceMessages producer messagesNumber "batch producer" |> Async.AwaitTask
            do! consumeMessages consumer messagesNumber "batch consumer" |> Async.AwaitTask

            Log.Debug("Finished 'Batch get sended if batch size exceeds'")

        }

        testAsync "Batch get sended if timeout exceeds" {

            Log.Debug("Started 'Batch get sended if timeout exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let batchSize = 10
            let messagesNumber = 5

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                    .CreateAsync() |> Async.AwaitTask

            do! fastProduceMessages producer messagesNumber "batch producer" |> Async.AwaitTask

            do! Async.Sleep 200

            do! consumeMessages consumer messagesNumber "batch consumer" |> Async.AwaitTask

            Log.Debug("Finished 'Batch get sended if timeout exceeds'")

        }

        testAsync "Batch get created from several tasks" {

            Log.Debug("Started 'Batch get created from several tasks'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync() |> Async.AwaitTask

            let taskData = createSendAndWaitTasks producer messagesNumber "batch producer"
            let tasks = taskData |> Array.map fst
            let sentMessages = taskData |> Array.map snd

            do! tasks |> Task.WhenAll |> Async.AwaitTask
            do! consumeAndVerifyMessages consumer "batch consumer" sentMessages |> Async.AwaitTask

            Log.Debug("Finished 'Batch get created from several tasks'")

        }
    ]
