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

    let client = getClient()

    let newGuid() = Guid.NewGuid().ToString("N")
    let getTopicName() = "public/default/topic-" + newGuid()
    let getProducerName() = sprintf "dlqProducer-%s" (newGuid())
    let getConsumerName() = sprintf "negativeConsumer-%s" (newGuid())
    let getDlqConsumerName() = sprintf "dlqConsumer-%s" (newGuid())
    let getDeadLettersPolicy() = DeadLettersPolicy(0, sprintf "public/default/topic-%s-DLQ" (newGuid()))

    let subscriptionName = "dlqSubscription"
    let numberOfMessages = 10

    let logTestStart testDescription = Log.Debug(sprintf "Started '%s'" testDescription)
    let logTestEnd testDescription = Log.Debug(sprintf "Finished '%s'" testDescription)

    let buildProducer(producerName, topicName) =
        ProducerBuilder(client)
            .ProducerName(producerName)
            .Topic(topicName)

    let buildConsumer(consumerName, topicName, deadLettersPolicy) =
        ConsumerBuilder(client)
            .ConsumerName(consumerName)
            .Topic(topicName)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(1.0))
            .DeadLettersPolicy(deadLettersPolicy)

    let buildDlqConsumer(consumerName, topicName) =
        ConsumerBuilder(client)
            .ConsumerName(consumerName)
            .Topic(topicName)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)

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

            let producerName = getProducerName()
            let consumerName = getConsumerName()
            let dlqConsumerName = getDlqConsumerName()
            let topicName = getTopicName()
            let policy = getDeadLettersPolicy()

            let! producer =
                buildProducer(producerName, topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer = buildConsumer(consumerName, topicName, policy).SubscribeAsync() |> Async.AwaitTask

            let! dlqConsumer =
                buildDlqConsumer(dlqConsumerName, policy.DeadLetterTopic).SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer numberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer numberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    Task.Delay(TimeSpan.FromSeconds(2.0))
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Failed messages stored in a default dead letter topic" {

            let description = "Failed messages stored in a default dead letter topic"

            description |> logTestStart

            let producerName = getProducerName()
            let consumerName = getConsumerName()
            let dlqConsumerName = getDlqConsumerName()
            let topicName = getTopicName()
            let policy = DeadLettersPolicy(0)

            let! producer =
                buildProducer(producerName, topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer = buildConsumer(consumerName, topicName, policy).SubscribeAsync() |> Async.AwaitTask

            let! dlqConsumer =
                buildDlqConsumer(dlqConsumerName, sprintf "%s-%s-DLQ" topicName subscriptionName)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer numberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer numberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    Task.Delay(TimeSpan.FromSeconds(2.0))
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Failed batch stored in a configured default letter topic" {

            let description = "Failed batch stored in a configured dead letter topic"

            description |> logTestStart

            let producerName = getProducerName()
            let consumerName = getConsumerName()
            let dlqConsumerName = getDlqConsumerName()
            let topicName = getTopicName()
            let policy = getDeadLettersPolicy()

            let! producer =
                buildProducer(producerName, topicName)
                    .BatchingMaxMessages(numberOfMessages)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer = buildConsumer(consumerName, topicName, policy).SubscribeAsync() |> Async.AwaitTask

            let! dlqConsumer =
                buildDlqConsumer(dlqConsumerName, policy.DeadLetterTopic).SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer numberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages dlqConsumer numberOfMessages dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    Task.Delay(TimeSpan.FromSeconds(2.0))
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }

        testAsync "Some failed batch messages get stored in a configured default letter topic" {

            let description = "Failed batch stored in a configured dead letter topic"

            description |> logTestStart

            let producerName = getProducerName()
            let consumerName = getConsumerName()
            let dlqConsumerName = getDlqConsumerName()
            let topicName = getTopicName()
            let policy = getDeadLettersPolicy()

            let! producer =
                buildProducer(producerName, topicName)
                    .BatchingMaxMessages(numberOfMessages)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer = buildConsumer(consumerName, topicName, policy).SubscribeAsync() |> Async.AwaitTask

            let! dlqConsumer =
                buildDlqConsumer(dlqConsumerName, policy.DeadLetterTopic).SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            if i = 5 || i = 6 then
                                do! consumer.NegativeAcknowledge(message.MessageId)
                            else
                                do! consumer.AcknowledgeAsync(message.MessageId)
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        for i in 5..6 do
                            let! message = dlqConsumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            do! dlqConsumer.AcknowledgeAsync(message.MessageId)
                            let expected = "Message #" + string i
                            if received.StartsWith(expected) |> not then
                                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received dlqConsumerName
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    Task.Delay(TimeSpan.FromSeconds(2.0))
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) |> Async.AwaitTask

            description |> logTestEnd
        }
    ]
