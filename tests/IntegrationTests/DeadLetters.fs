module Pulsar.Client.IntegrationTests.DeadLetters

open System
open Expecto
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System.Text

[<Tests>]
let tests =

    let logTestStart testDescription = Log.Debug(sprintf "Started '%s'" testDescription)
    let logTestEnd testDescription = Log.Debug(sprintf "Finished '%s'" testDescription)
    let createProducer() = getClient() |> ProducerBuilder
    let createConsumer() = getClient() |> ConsumerBuilder

    let getTestConfig() =
        let newGuid = Guid.NewGuid().ToString("N")
        {|
            TopicName = sprintf "public/default/topic-%s" newGuid
            DeadLettersPolicy = DeadLettersPolicy(0, sprintf "public/default/topic-%s-DLQ" newGuid)
            SubscriptionName = "dlqSubscription"
            NumberOfMessages = 10
        |}

    let receiveAndAckNegative (consumer: IConsumer) number =
        task {
            for _ in 1..number do
                let! message = consumer.ReceiveAsync()
                do! consumer.NegativeAcknowledge(message.MessageId)
        }

    testList "deadLetters" [
        testAsync "Failed messages stored in a configured dead letter topic" {

            let description = "Failed messages stored in a configured dead letter topic"

            description |> logTestStart

            let config = getTestConfig()
            let producerName = "configuredProducer"
            let consumerName = "configuredConsumer"
            let dlqConsumerName = "configuredDLQConsumer"

            let! producer =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .EnableBatching(false)
                    .CreateAsync()
                    |> Async.AwaitTask

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLettersPolicy(config.DeadLettersPolicy)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer config.NumberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer config.NumberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer config.NumberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Failed messages stored in a default dead letter topic" {

            let description = "Failed messages stored in a default dead letter topic"

            description |> logTestStart

            let config = getTestConfig()
            let producerName = "defaultProducer"
            let consumerName = "defaultConsumer"
            let dlqConsumerName = "defaultDLQConsumer"

            let! producer =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .EnableBatching(false)
                    .CreateAsync()
                    |> Async.AwaitTask

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLettersPolicy(DeadLettersPolicy(0))
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(sprintf "%s-%s-DLQ" config.TopicName config.SubscriptionName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer config.NumberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer config.NumberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer config.NumberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Failed batch stored in a configured default letter topic" {

            let description = "Failed batch stored in a configured dead letter topic"

            description |> logTestStart

            let config = getTestConfig()
            let producerName = "configuredBatchProducer"
            let consumerName = "configuredBatchConsumer"
            let dlqConsumerName = "configuredBatchDLQConsumer"

            let! producer =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .BatchingMaxMessages(config.NumberOfMessages)
                    .CreateAsync()
                    |> Async.AwaitTask

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLettersPolicy(config.DeadLettersPolicy)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer config.NumberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer config.NumberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer config.NumberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Some failed batch messages get stored in a configured default letter topic" {

            let description = "Failed batch stored in a configured dead letter topic"

            description |> logTestStart

            let config = getTestConfig()
            let producerName = "someBatchProducer"
            let consumerName = "someBatchConsumer"
            let dlqConsumerName = "someBatchDLQConsumer"

            let lBorder = 5
            let uBorder = 6
            let redeliveryCount = 1

            let! producer =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .BatchingMaxMessages(config.NumberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                    .CreateAsync()
                    |> Async.AwaitTask

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLettersPolicy(DeadLettersPolicy(redeliveryCount, config.DeadLettersPolicy.DeadLetterTopic))
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer config.NumberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 0..redeliveryCount do
                            for i in 1..config.NumberOfMessages do
                                let! message = consumer.ReceiveAsync()
                                if i >= lBorder && i <= uBorder then
                                    do! consumer.NegativeAcknowledge(message.MessageId)
                                else
                                    do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        for i in lBorder..uBorder do
                            let! message = dlqConsumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Data)
                            do! dlqConsumer.AcknowledgeAsync(message.MessageId)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith
                                <| sprintf
                                    "Incorrect message expected %s received %s consumer %s"
                                    expected
                                    received
                                    dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }
    ]
