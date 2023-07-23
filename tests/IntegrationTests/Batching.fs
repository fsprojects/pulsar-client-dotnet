module Pulsar.Client.IntegrationTests.Batching

open System
open System.Diagnostics
open Expecto
open Expecto.Flip
open FSharp.UMX
open Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System.Text


[<Tests>]
let tests =


    testList "Batching" [

        testTask "Batch get sended if batch size exceeds" {

            Log.Debug("Started 'Batch get sended if batch size exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync()

            do! fastProduceMessages producer messagesNumber "batch producer"
            do! consumeMessages consumer messagesNumber "batch consumer"

            Log.Debug("Finished 'Batch get sended if batch size exceeds'")

        }

        testTask "Batch get sended if timeout exceeds" {

            Log.Debug("Started 'Batch get sended if timeout exceeds'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let batchSize = 10
            let messagesNumber = 5

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                    .CreateAsync()

            do! fastProduceMessages producer messagesNumber "batch producer"

            do! Task.Delay 200

            do! consumeMessages consumer messagesNumber "batch consumer"

            Log.Debug("Finished 'Batch get sended if timeout exceeds'")

        }

        testTask "Batch get created from several tasks" {

            Log.Debug("Started 'Batch get created from several tasks'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(messagesNumber)
                    .CreateAsync()

            let taskData = createSendAndWaitTasks producer messagesNumber "batch producer"
            let tasks = taskData |> Array.map fst
            let sentMessages = taskData |> Array.map snd

            do! tasks |> Task.WhenAll
            do! consumeAndVerifyMessages consumer "batch consumer" sentMessages

            Log.Debug("Finished 'Batch get created from several tasks'")
        }

        testTask "Keys and properties are propertly passed with default batching" {

            Log.Debug("Started Keys and properties are propertly passed with default batching")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "propsTestProducer"
            let consumerName = "propsTestConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
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
                        do! produceMessagesWithProps producer 100 producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithProps consumer 100 consumerName
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Keys and properties are propertly passed with default batching")
        }

        testTask "Keys and properties are propertly passed with key-based batching" {

            Log.Debug("Started Keys and properties are propertly passed with key-based batching")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "propsTestProducer"
            let consumerName = "propsTestConsumer"
            let numberOfMessages = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                    .BatchBuilder(BatchBuilder.KeyBased)
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producer1Task =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessagesWithSameKey producer numberOfMessages "key1" (producerName + "1")
                    }:> Task)

            let producer2Task =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessagesWithSameKey producer numberOfMessages "key2" (producerName + "2")
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages consumerName
                        do! consumeMessages consumer numberOfMessages consumerName
                    }:> Task)

            do! Task.WhenAll(producer1Task, producer2Task, consumerTask)
            Log.Debug("Finished Keys and properties are propertly passed with key-based batching")
        }

        testTask "Batch recieve works with regular consumer"{
            Log.Debug("Started Batch recieve works with regular consumer")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "batchRecieveProducer"
            let consumerName = "batchRecieveConsumer"
            let numberOfMessages = 10
            let batchTimeout = TimeSpan.FromSeconds(2.0)

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .BatchReceivePolicy(BatchReceivePolicy(8, -1L, batchTimeout))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let sw = Stopwatch()
                        sw.Start()
                        let! messagesBatch = consumer.BatchReceiveAsync()
                        let firstBatchTime = sw.Elapsed
                        if firstBatchTime > TimeSpan.FromSeconds(0.5) then
                                failwith <| sprintf "Too long to receive first batch consumer %s passed %f ms" consumerName firstBatchTime.TotalMilliseconds
                        let mutable i = 1
                        for message in messagesBatch do
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            Log.Debug("{0} acknowledged {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            i <- i + 1
                        sw.Restart()
                        let! messagesBatch2 = consumer.BatchReceiveAsync()
                        let secondBatchTime = sw.Elapsed
                        if secondBatchTime < (batchTimeout - TimeSpan.FromMilliseconds(15.0)) then
                                failwith <| sprintf "Too fast to get second batch consumer %s passed %f ms" consumerName secondBatchTime.TotalMilliseconds
                        for message in messagesBatch2 do
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            Log.Debug("{0} acknowledged {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            i <- i + 1

                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Batch recieve works with regular consumer")
        }

        testTask "Second batch is formed well after the first one" {

            Log.Debug("Started 'Second batch is formed well after the first one'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 5

            let! (consumer: IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("batch consumer")
                    .SubscriptionName("batch-subscription")
                    .SubscribeAsync()

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("batch producer")
                    .EnableBatching(true)
                    .BatchingMaxMessages(messagesNumber / 2)
                    .BatchingMaxBytes(100)
                    .MaxPendingMessages(1)
                    .BlockIfQueueFull(true)
                    .CreateAsync()

            for i in 0 .. messagesNumber-1 do
                let buffer = Array.zeroCreate<byte> 50
                Random.Shared.NextBytes(buffer)
                producer.SendAsync(buffer) |> ignore

            for i in 0 .. messagesNumber-1 do
                let! (message: Message<byte[]>) = consumer.ReceiveAsync()
                match message.MessageId.Type with
                | Batch (index, _) ->
                    Expect.equal $"Run {i} failed" (i % 2) %index
                | _ ->
                    failwith "Expected batch message"

            Log.Debug("Finished 'Second batch is formed well after the first one'")

        }
    ]
