module Pulsar.Client.IntegrationTests.Acks

open System
open Expecto
open Expecto.Flip

open System.Text
open System.Threading.Tasks
open System.Threading
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open System.Collections.Generic
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    testList "Acks" [

        testTask "Messages get redelivered if ackTimeout is set" {

            Log.Debug("Started messages get redelivered if ackTimeout is set")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "AckTimeoutConsumerWithBatching"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .AckTimeout(TimeSpan.FromSeconds(1.0))
                    .SubscribeAsync()

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
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            if (i <= 90) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        do! Task.Delay(1100)
                        for i in [91..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished messages get redelivered if ackTimeout is set")

        }

        testTask "Messages get redelivered if ackTimeout is set without batching" {

            Log.Debug("Started messages get redelivered if ackTimeout is set without batching")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "AckTimeoutConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .AckTimeout(TimeSpan.FromSeconds(2.0))
                    .SubscribeAsync()

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
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            if (i <= 90) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        do! Task.Delay(2100)
                        for i in [91..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished messages get redelivered if ackTimeout is set without batching")

        }

        testTask "Messages get redelivered if ackTimeout is set for shared subscription" {

            Log.Debug("Started messages get redelivered if ackTimeout is set for shared subscription")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName("AckTimeoutConsumerShared")
                    .AckTimeout(TimeSpan.FromSeconds(1.0))
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

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
                            let received = Encoding.UTF8.GetString(message.Data)
                            let receivedNumber = getMessageNumber received
                            hashSet1.Remove receivedNumber |> ignore
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            if (i <= 90) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        Expect.isEmpty "" hashSet1
                        do! Task.Delay(1100)
                        for _ in [91..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            let receivedNumber = getMessageNumber received
                            hashSet2.Remove receivedNumber |> ignore
                            Log.Debug("{0} received {1}", "AckTimeoutConsumer", received)
                            do! consumer.AcknowledgeAsync(message.MessageId)
                        Expect.isEmpty "" hashSet2
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished messages get redelivered if ackTimeout is set for shared subscription")

        }

        testTask "Cumulative ack works well" {

            Log.Debug("Started Cumulative ack works well")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "CumulativeAcker"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let mutable messageId = MessageId.Earliest
                        for i in [1..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            messageId <- message.MessageId
                        do! consumer.AcknowledgeCumulativeAsync(messageId)
                        do! Task.Delay(100)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            let cts = new CancellationTokenSource()
            cts.CancelAfter(200)
            let t = Task.Run((fun () -> (task {
                                                do! consumer.RedeliverUnacknowledgedMessagesAsync()
                                                let! _ = consumer.ReceiveAsync()
                                                ()
                                            }).Wait(cts.Token)), cts.Token)
            Expect.throwsT2<TaskCanceledException> (fun () ->
                t.Wait()
            ) |> ignore
            cts.Dispose()
            Log.Debug("Finished Cumulative ack works well")

        }

        testTask "Individual ack works for batch messages" {

            Log.Debug("Started Individual ack works for batch messages")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "CumulativeAcker"
            let producerName = "BatchingProducer"
            let messagesCount = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .ProducerName(producerName)
                    .BatchingMaxMessages(10)
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer messagesCount ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer messagesCount ""
                        do! Task.Delay(100)
                    }:> Task)

            let! _ = Task.WhenAll(producerTask, consumerTask)

            let cts = new CancellationTokenSource()
            cts.CancelAfter(200)
            let t = Task.Run((fun () -> (task {
                                                do! consumer.RedeliverUnacknowledgedMessagesAsync()
                                                let! _ = consumer.ReceiveAsync()
                                                ()
                                            }).Wait(cts.Token)), cts.Token)
            Expect.throwsT2<TaskCanceledException> (fun () ->
                t.Wait()
            ) |> ignore
            cts.Dispose()

            Log.Debug("Finished Individual ack works for batch messages")
        }

        testTask "Cumulative ack work for batch messages" {

            Log.Debug("Started Cumulative ack works for batch messages")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "CumulativeAcker"
            let producerName = "BatchingProducer"
            let messagesCount = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .ProducerName(producerName)
                    .BatchingMaxMessages(10)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(25.0))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer messagesCount ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in [1..messagesCount] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            if i%2 = 0 then
                                do! consumer.AcknowledgeCumulativeAsync(message.MessageId)
                                do! Task.Delay(50)
                        do! consumer.RedeliverUnacknowledgedMessagesAsync()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            let cts = new CancellationTokenSource()
            cts.CancelAfter(200)
            let t = Task.Run((fun () -> (task {
                                                do! consumer.RedeliverUnacknowledgedMessagesAsync()
                                                let! _ = consumer.ReceiveAsync()
                                                ()
                                            }).Wait(cts.Token)), cts.Token)
            Expect.throwsT2<TaskCanceledException> (fun () ->
                t.Wait()
            ) |> ignore
            cts.Dispose()

            Log.Debug("Finished Cumulative ack works for batch messages")
        }

        testTask "Negative acks work correctly" {

            Log.Debug("Started Negative acks work correctly")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "NegativeAcksConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromMilliseconds(100.0))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 10 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            if (i < 10) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                            else
                                do! consumer.NegativeAcknowledge(message.MessageId)
                        for i in [10..10] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Negative acks work correctly")

        }

        testTask "Negative acks work correctly with batches" {

            Log.Debug("Started Negative acks work correctly with batches")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "BatchNegativeAcksConsumer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(150.0))
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .EnableBatchIndexAcknowledgment(true)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromMilliseconds(150.0))
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer 10 ""
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            if (i < 10) then
                                do! consumer.AcknowledgeAsync(message.MessageId)
                            else
                                do! consumer.NegativeAcknowledge(message.MessageId)
                        for i in [10..10] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)

            Log.Debug("Finished Negative acks work correctly with batches")

        }

        testTask "Messages get acked on consumer close" {

            Log.Debug("Started messages get redelivered if ackTimeout is set")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "AckOnCloseConsumer"

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! (consumer1 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscribeAsync()

            let data = [| 1uy |]
            let! _ = producer.SendAsync(data)

            let! (message : Message<byte[]>) = consumer1.ReceiveAsync()
            do! consumer1.AcknowledgeAsync(message.MessageId)

            Expect.sequenceEqual "" message.Data data
            do! consumer1.DisposeAsync().AsTask()

            let! (consumer2 : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscribeAsync()
            let cts = new CancellationTokenSource(200)
            try
                let! msg = consumer2.ReceiveAsync(cts.Token)
                failwith <| "Unexpected success" + msg.ToString()
            with
            | :? TaskCanceledException -> ()
            | ex ->
                reraize ex

            Log.Debug("Finished Messages get acked on consumer close")

        }
    ]
