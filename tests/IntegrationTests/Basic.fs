module Pulsar.Client.IntegrationTests.Basic

open System
open System.Text.Json
open System.Threading
open System.Diagnostics

open Expecto
open Expecto.Flip

open System.Text
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

[<Tests>]
let tests =

    testList "Basic" [

        testTask "Sent messageId should be equal to received messageId" {

            Log.Debug("Started Sent messageId should be equal to received messageId")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let! msg1Id = producer1.SendAsync([| 0uy |])
            let! msg2Id = producer2.SendAsync([| 1uy |])
            let! (msg1 : Message<byte[]>) = consumer.ReceiveAsync()
            let! (msg2 : Message<byte[]>) = consumer.ReceiveAsync()

            Expect.isTrue "" (msg1Id = msg1.MessageId)
            Expect.equal "" [| 0uy |] <| msg1.GetValue()

            Expect.isTrue "" (msg2Id = msg2.MessageId)
            Expect.equal "" [| 1uy |] <| msg2.GetValue()

            Log.Debug("Finished Sent messageId should be equal to received messageId")
        }

        testTask "Send and receive 100 messages concurrently works fine in default configuration" {

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages "concurrent"
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine in default configuration")
        }

        testTask "Send 100 messages and then receiving them works fine when retention is set on namespace" {

            Log.Debug("Started send 100 messages and then receiving them works fine when retention is set on namespace")
            let client = getClient()
            let topicName = "public/retention/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync()

            do! produceMessages producer 100 "sequential"

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("sequential")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync()

            do! consumeMessages consumer 100 "sequential"
            Log.Debug("Finished send 100 messages and then receiving them works fine when retention is set on namespace")
        }

        testTask "Full roundtrip (emulate Request-Response behaviour)" {

            Log.Debug("Started Full roundtrip (emulate Request-Response behaviour)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let! (consumer1 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName2)
                    .ConsumerName("consumer1")
                    .SubscriptionName("my-subscriptionx")
                    .SubscribeAsync()

            let! (consumer2 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName1)
                    .ConsumerName("consumer2")
                    .SubscriptionName("my-subscriptiony")
                    .SubscribeAsync()

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName1)
                    .ProducerName("producer1")
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName2)
                    .ProducerName("producer2")
                    .CreateAsync()

            let t1 = Task.Run(fun () ->
                fastProduceMessages producer1 messagesNumber "producer1" |> Task.WaitAll
                Log.Debug("t1 ended")
            )

            let t2 = Task.Run(fun () ->
                consumeMessages consumer1 messagesNumber "consumer1" |> Task.WaitAll
                Log.Debug("t2 ended")
            )

            let t3 = Task.Run(fun () ->
                task {
                    for i in 1..messagesNumber do
                        let! message = consumer2.ReceiveAsync()
                        let received = Encoding.UTF8.GetString(message.Data)
                        do! consumer2.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} received {1}", "consumer2", received)
                        let expected = "Message #" + string i
                        if received.StartsWith(expected) |> not then
                            failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "consumer2"
                        let! _ = producer2.SendAndForgetAsync(message.Data)
                        ()
                } :> Task
            )
            do! [|t1; t2; t3|] |> Task.WhenAll

            Log.Debug("Finished Full roundtrip (emulate Request-Response behaviour)")
        }

        testTask "Concurrent send and receive work fine" {

            Log.Debug("Started Concurrent send and receive work fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 100
            let producerName = "concurrentProducer"
            let consumerName = "concurrentConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTasks =
                [| 1..3 |]
                |> Array.map (fun _ ->
                    Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages producerName
                        } :> Task))

            let mutable processedCount = 0
            let consumerTask i =
                fun () ->
                    task {
                        while true do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0}-{1} received {2}", consumerName, i, received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            Log.Debug("{0}-{1} acknowledged {2}", consumerName, i, received)
                            if Interlocked.Increment(&processedCount) = (numberOfMessages*3) then
                                do! consumer.DisposeAsync()
                    } :> Task
            let consumerTasks =
                [| 1..3 |]
                |> Array.map (fun i -> Task.Run(consumerTask i))

            let resultTasks = Array.append consumerTasks producerTasks
            try
                do! Task.WhenAll(resultTasks)
            with
            | :? AlreadyClosedException ->
                ()
            | ex ->
                failtestf "Incorrect exception type %A" (ex.GetType().FullName)
            Log.Debug("Finished Concurrent send and receive work fine")
        }

        testTask "Client, producer and consumer can't be accessed after close" {

            Log.Debug("Started 'Client, producer and consumer can't be accessed after close'")

            let client = getNewClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer1 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing1-subscription")
                    .SubscribeAsync()

            let! (consumer2 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing2-subscription")
                    .SubscribeAsync()

            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer1")
                    .CreateAsync()

            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("ClosingProducer2")
                    .CreateAsync()

            do! consumer1.DisposeAsync().AsTask()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer1.ReceiveAsync().Result |> ignore) |> ignore
            do! producer1.DisposeAsync().AsTask()
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAndForgetAsync([||]).Result) |> ignore
            do! client.CloseAsync()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAndForgetAsync([||]).Result) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.CloseAsync().Result) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
        }

        testTask "Scheduled message should be delivered at requested time" {

            Log.Debug("Started 'Scheduled message should be delivered at requested time'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let interval = 10000L
            let producerName = "schedule-producer"
            let consumerName = "schedule-consumer"
            let testEventTime = DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc) |> convertToMsTimestamp
            let sw = Stopwatch()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName).EnableBatching(false)
                    .ProducerName(producerName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("schedule-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let now = DateTime.UtcNow;
                        let deliverAt = now.AddMilliseconds(float interval) |> convertToMsTimestamp
                        let timestamp = Nullable(%deliverAt)
                        let message = Encoding.UTF8.GetBytes(sprintf "Message was sent with interval '%i' milliseconds" interval)
                        sw.Start()
                        let! _ = producer.NewMessage(message, deliverAt = timestamp, eventTime = (%testEventTime |> Nullable)) |> producer.SendAsync
                        ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! message = consumer.ReceiveAsync()
                        let received = Encoding.UTF8.GetString(message.Data)
                        Log.Debug("{0} received {1}", consumerName, received)
                        Expect.equal "" %testEventTime (message.EventTime.GetValueOrDefault())
                        sw.Stop()
                        do! consumer.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} acknowledged {1}", consumerName, received)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            let elapsed = sw.ElapsedMilliseconds

            interval - elapsed
            |> Math.Abs
            |> (>) 4000L
            |> Expect.isTrue (sprintf "Message delivered in unexpected interval %i while should be %i" elapsed interval)

            Log.Debug("Finished 'Scheduled message should be delivered at requested time'")
        }

        testTask "Create the replicated subscription should be successful" {
            Log.Debug("Started 'Create the replicated subscription should be successful'")
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "replicated-consumer"
            let client = getClient()
            let! (_ : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("replicate")
                    .SubscriptionType(SubscriptionType.Shared)
                    .ReplicateSubscriptionState(true)
                    .SubscribeAsync()

            do! Task.Delay 1000

            let url = $"{pulsarHttpAddress}/admin/v2/persistent/" + topicName + "/stats"
            let! (response: string) = commonHttpClient.GetStringAsync(url)
            let json = JsonDocument.Parse(response)
            let isReplicated = json.RootElement.GetProperty("subscriptions").GetProperty("replicate").GetProperty("isReplicated").GetBoolean()
            Expect.isTrue "" isReplicated
            Log.Debug("Finished 'Create the replicated subscription should be successful'")
        }

#if !NOTLS
        // Before running this test set 'maxMessageSize' for broker and 'nettyMaxFrameSizeBytes' for bookkeeper
        testTask "Send large message works fine" {

            Log.Debug("Started Send large message works fine")
            let client = getSslAdminClient()

            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("bigMessageProducer")
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("bigMessageConsumer")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let message = Array.create 10_400_000 1uy
                        let! _ = producer.SendAsync(message)
                        ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! message = consumer.ReceiveAsync()
                        if not (message.Data |> Array.forall (fun x -> x = 1uy)) then
                            failwith "incorrect message received"
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Send large message works fine")
        }
#endif
    ]
