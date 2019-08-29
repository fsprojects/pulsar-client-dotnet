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

        testCase "Batch get sended if batch size exceeds" <| fun () ->

            Log.Debug("Started 'Batch get sended if batch size exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()
                    .Result

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching()
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync()
                    .Result

            fastProduceMessages producer messagesNumber "batch producer" |> Task.WaitAll
            consumeMessages consumer messagesNumber "batch consumer" |> Task.WaitAll

            Log.Debug("Finished 'Batch get sended if batch size exceeds'")

        testCase "Batch get sended if timeout exceeds" <| fun () ->

            Log.Debug("Started 'Batch get sended if timeout exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let batchSize = 10
            let messagesNumber = 5

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()
                    .Result

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching()
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                    .CreateAsync()
                    .Result

            fastProduceMessages producer messagesNumber "batch producer" |> Task.WaitAll

            Task.Delay(TimeSpan.FromMilliseconds(200.0)).Wait()

            consumeMessages consumer messagesNumber "batch consumer" |> Task.WaitAll

            Log.Debug("Finished 'Batch get sended if timeout exceeds'")

        testCase "Batch get created from several tasks" <| fun () ->

            Log.Debug("Started 'Batch get created from several tasks'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()
                    .Result

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching()
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync()
                    .Result

            let taskData = createSendAndWaitTasks producer messagesNumber "batch producer"
            let tasks = taskData |> Array.map fst
            let sentMessages = taskData |> Array.map snd

            tasks |> Task.WaitAll
            consumeAndVerifyMessages consumer "batch consumer" sentMessages |> Task.WaitAll

            Log.Debug("Finished 'Batch get created from several tasks'")
    ]
