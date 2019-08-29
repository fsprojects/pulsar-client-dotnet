module Pulsar.Client.IntegrationTests.Acks

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open System.Threading
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

    testList "acks" [

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

        ftestCase "Cumulative ack works well" <| fun () ->

            Log.Debug("Started Cumulative ack works well")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName = "CumulativeAcker"

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .SubscriptionType(SubscriptionType.Exclusive)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
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
                        let mutable messageId = MessageId.Earliest
                        for i in [1..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            Log.Debug("{0} received {1}", consumerName, received)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
                            messageId <- message.MessageId
                        do! consumer.AcknowledgeCumulativeAsync(messageId)
                        do! Task.Delay(100)
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            use cts = new CancellationTokenSource()
            cts.CancelAfter(200)
            let t = Task.Run((fun () -> (task {
                                                do! consumer.RedeliverUnacknowledgedMessagesAsync()
                                                let! _ = consumer.ReceiveAsync()
                                                ()
                                            }).Wait(cts.Token)), cts.Token)
            Expect.throwsT2<TaskCanceledException> (fun () ->
                t.Wait()
            ) |> ignore

            Log.Debug("Finished Cumulative ack works well")
    ]
