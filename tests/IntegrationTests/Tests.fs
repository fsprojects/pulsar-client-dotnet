module Tests

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
open FSharp.UMX


[<Literal>]
let pulsarAddress = "pulsar://my-pulsar-cluster:31002"

let configureLogging() =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Warning()
            .WriteTo.Console(theme = AnsiConsoleTheme.Code, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3} {ThreadId}] {Message:lj}{NewLine}{Exception}")
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .CreateLogger()
    let serviceCollection = new ServiceCollection()
    let sp =
        serviceCollection
            .AddLogging(fun configure -> configure.AddSerilog(dispose = true) |> ignore)
            .BuildServiceProvider()
    let logger = sp.GetService<ILogger<PulsarClient>>()
    PulsarClient.Logger <- logger

[<Tests>]
let tests =

    configureLogging()

    let getClient() =
        PulsarClientBuilder()
            .WithServiceUrl(pulsarAddress)
            .Build()

    let produceMessages (producer: Producer) number producerName =
        task {
            for i in [1..number] do
                let! _ = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
                ()
        }

    let fastProduceMessages (producer: Producer) number producerName =
        task {
            for i in [1..number] do
                let! _ = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
                ()
        }

    let createSendAndWaitTasks (producer: Producer) number producerName =
        let createTask taskNumber =
            let message = sprintf "Message #%i Sent from %s on %s" taskNumber producerName (DateTime.Now.ToLongTimeString())
            let messageBytes = Encoding.UTF8.GetBytes(message)
            let task = Task.Run(fun() -> producer.SendAndWaitAsync(messageBytes) |> ignore)
            (task, message)

        [|1..number|] |> Array.map createTask

    let getMessageNumber (msg: string) =
        let ind1 = msg.IndexOf("#")
        let ind2 = msg.IndexOf("Sent")
        let subString = msg.Substring(ind1+1, ind2 - ind1 - 2)
        int subString

    let consumeMessages (consumer: Consumer) number consumerName =
        task {
            for i in [1..number] do
                let! message = consumer.ReceiveAsync()
                let received = Encoding.UTF8.GetString(message.Payload)
                do! consumer.AcknowledgeAsync(message.MessageId)
                Log.Debug("{0} received {1}", consumerName, received)
                let expected = "Message #" + string i
                if received.StartsWith(expected) |> not then
                    failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
        }

    let consumeAndVerifyMessages (consumer: Consumer) consumerName (expectedMessages : string[]) =
        task {
            for i in [1..expectedMessages.Length] do
                let! message = consumer.ReceiveAsync()
                let received = Encoding.UTF8.GetString(message.Payload)
                do! consumer.AcknowledgeAsync(message.MessageId)
                Log.Debug("{0} received {1}", consumerName, received)
                if expectedMessages |> Array.contains received |> not then
                    failwith <| sprintf "Received unexpected message '%s' consumer %s" received consumerName
        }

    testList "basic" [
        testCase "Send and receive 100 messages concurrently works fine in default configuration" <| fun () ->

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
                    .Result

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 "concurrent"
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine in default configuration")

        testCase "Send 100 messages and then receiving them works fine when retention is set on namespace" <| fun () ->

            Log.Debug("Started send 100 messages and then receiving them works fine when retention is set on namespace")
            let client = getClient()
            let topicName = "public/retention/topic-" + Guid.NewGuid().ToString("N")

            let producer =
                ProducerBuilder(client)
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            (produceMessages producer 100 "sequential").Wait()

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("sequential")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync()
                    .Result

            (consumeMessages consumer 100 "sequential").Wait()
            Log.Debug("Finished send 100 messages and then receiving them works fine when retention is set on namespace")

        testCase "Full roundtrip (emulate Request-Response behaviour)" <| fun () ->

            Log.Debug("Started Full roundtrip (emulate Request-Response behaviour)")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName2)
                    .ConsumerName("consumer1")
                    .SubscriptionName("my-subscriptionx")
                    .SubscribeAsync()
                    .Result

            let consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName1)
                    .ConsumerName("consumer2")
                    .SubscriptionName("my-subscriptiony")
                    .SubscribeAsync()
                    .Result

            let producer1 =
                ProducerBuilder(client)
                    .Topic(topicName1)
                    .ProducerName("producer1")
                    .CreateAsync()
                    .Result

            let producer2 =
                ProducerBuilder(client)
                    .Topic(topicName2)
                    .ProducerName("producer2")
                    .CreateAsync()
                    .Result

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
                        let received = Encoding.UTF8.GetString(message.Payload)
                        do! consumer2.AcknowledgeAsync(message.MessageId)
                        Log.Debug("{0} received {1}", "consumer2", received)
                        let expected = "Message #" + string i
                        if received.StartsWith(expected) |> not then
                            failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "consumer2"
                        let! _ = producer2.SendAsync(message.Payload)
                        ()
                } |> Task.WaitAll
                Log.Debug("t3 ended")
            )
            [|t1; t2; t3|] |> Task.WaitAll

            Log.Debug("Finished Full roundtrip (emulate Request-Response behaviour)")

        testCase "Send and receive 100 messages concurrently works fine with small receiver queue size" <| fun () ->

            Log.Debug("Started Send and receive 100 messages concurrently works fine with small receiver queue size")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(10)
                    .SubscribeAsync()
                    .Result

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

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine with small receiver queue size")

        testCase "Messages get redelivered if ackTimeout is set" <| fun () ->

            Log.Debug("Started messages get redelivered if ackTimeout is set")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName("AckTimeoutConsumer")
                    .AckTimeout(TimeSpan.FromSeconds(1.0))
                    .SubscribeAsync()
                    .Result

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in [1..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "AckTimeoutConsumer"
                            if (i <= 90) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        do! Task.Delay(1100)
                        for i in [91..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received "AckTimeoutConsumer"
                            do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished messages get redelivered if ackTimeout is set")

        testCase "Messages get redelivered if ackTimeout is set for shared subscription" <| fun () ->

            Log.Debug("Started messages get redelivered if ackTimeout is set for shared subscription")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName("AckTimeoutConsumerShared")
                    .AckTimeout(TimeSpan.FromSeconds(1.0))
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    .Result

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let hashSet1 = HashSet [1..100]
                        let hashSet2 = HashSet [91..100]
                        for i in [1..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            let receivedNumber = getMessageNumber received
                            hashSet1.Remove receivedNumber |> ignore
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            if (i <= 90) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        Expect.isEmpty "" hashSet1
                        do! Task.Delay(1100)
                        for i in [91..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            let receivedNumber = getMessageNumber received
                            hashSet2.Remove receivedNumber |> ignore
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                        Expect.isEmpty "" hashSet2
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished messages get redelivered if ackTimeout is set for shared subscription")

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

        testCase "Client, producer and consumer can't be accessed after close" <| fun () ->

            Log.Debug("Started 'Client, producer and consumer can't be accessed after close'")

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let messagesNumber = 100

            let consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing1-subscription")
                    .SubscribeAsync()
                    .Result

            let consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("ClosingConsumer")
                    .SubscriptionName("closing2-subscription")
                    .SubscribeAsync()
                    .Result

            let producer1 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("ClosingProducer1")
                    .CreateAsync()
                    .Result

            let producer2 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("ClosingProducer2")
                    .CreateAsync()
                    .Result

            consumer1.CloseAsync().Wait()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer1.ReceiveAsync().Result |> ignore) |> ignore
            producer1.CloseAsync().Wait()
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer1.SendAsync([||]).Result |> ignore) |> ignore
            client.CloseAsync().Wait()
            Expect.throwsT2<AlreadyClosedException> (fun () -> consumer2.UnsubscribeAsync().Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> producer2.SendAsync([||]).Result |> ignore) |> ignore
            Expect.throwsT2<AlreadyClosedException> (fun () -> client.GetPartitionedTopicMetadata(%"abc").Result |> ignore) |> ignore

            Log.Debug("Finished 'Client, producer and consumer can't be accessed after close'")
    ]
