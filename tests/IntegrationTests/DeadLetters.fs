module Pulsar.Client.IntegrationTests.DeadLetters

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System.Text
open FSharp.UMX

[<Tests>]
let tests =

    let logTestStart testDescription = Log.Debug(sprintf "Started '%s'" testDescription)
    let logTestEnd testDescription = Log.Debug(sprintf "Finished '%s'" testDescription)
    let createProducer() = getClient().NewProducer()
    let createConsumer() = getClient().NewConsumer()

    let getTestConfig() =
        let newGuid = Guid.NewGuid().ToString("N")
        {|
            TopicName = sprintf "public/default/topic-%s" newGuid
            DeadLettersPolicy = DeadLetterPolicy(0, sprintf "public/default/topic-%s-DLQ" newGuid)
            SubscriptionName = "test-subscription"
            NumberOfMessages = 10
        |}

    let getTestConfigForPartitionedTopic() =
        let topic = "public/default/partitioned-dl-test"
        {|
            TopicName = topic
            DeadLettersPolicy = DeadLetterPolicy(0, sprintf "%s-DLQ" topic)
            SubscriptionName = "test-subscription"
            NumberOfMessages = 10
        |}

    let receiveAndAckNegative (consumer: IConsumer<'T>) number =
        task {
            for _ in 1..number do
                let! message = consumer.ReceiveAsync()
                do! consumer.NegativeAcknowledge(message.MessageId)
        }

    testList "deadLetters" [
        testTask "Failed messages stored in a configured dead letter topic" {

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

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLetterPolicy(config.DeadLettersPolicy)
                    .SubscribeAsync()

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()
                    
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

            do! Task.WhenAll(tasks) 

            description |> logTestEnd
        }
        
        testTask "Partitioned topic failed messages stored in a configured dead letter topic" {

            let description = "Failed messages in a partitioned topic are stored in a configured dead letter topic"

            description |> logTestStart

            let config = getTestConfigForPartitionedTopic()
            let producerName = "configuredProducer"
            let consumerName = "configuredConsumer"
            let dlqConsumerName = "configuredDLQConsumer"

            let! producer =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromMilliseconds(100.0))
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .DeadLetterPolicy(config.DeadLettersPolicy)
                    .SubscribeAsync()

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

            let messages = generateMessages config.NumberOfMessages producerName
            
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer messages
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! receiveAndAckNegative consumer config.NumberOfMessages
                    }:> Task)

            let dlqConsumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeAndVerifyMessages dlqConsumer dlqConsumerName messages
                    }:> Task)

            let tasks =
                [|
                    producerTask
                    consumerTask
                    dlqConsumerTask
                |]

            do! Task.WhenAll(tasks) 
            do! Task.Delay(110) // wait for acks

            description |> logTestEnd
        }

        testTask "Failed messages stored in a default dead letter topic" {

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

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLetterPolicy(DeadLetterPolicy(0))
                    .SubscribeAsync()

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(sprintf "%s-%s-DLQ" config.TopicName config.SubscriptionName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

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

            do! Task.WhenAll(tasks) 

            description |> logTestEnd
        }

        testTask "Failed batch stored in a configured default letter topic" {

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

            let! consumer =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLetterPolicy(config.DeadLettersPolicy)
                    .SubscribeAsync()

            let! dlqConsumer =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

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

            do! Task.WhenAll(tasks) 

            description |> logTestEnd
        }

        testTask "Some failed batch messages get stored in a configured default letter topic" {

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

            let! (consumer : IConsumer<byte[]>) =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(0.5))
                    .DeadLetterPolicy(DeadLetterPolicy(redeliveryCount, config.DeadLettersPolicy.DeadLetterTopic))
                    .SubscribeAsync()

            let! (dlqConsumer : IConsumer<byte[]>) =
                createConsumer()
                    .ConsumerName(dlqConsumerName)
                    .Topic(config.DeadLettersPolicy.DeadLetterTopic)
                    .SubscriptionName(config.SubscriptionName)
                    .SubscriptionType(SubscriptionType.Shared)
                    .SubscribeAsync()

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

            do! Task.WhenAll(tasks) 

            description |> logTestEnd
        }
        
        testTask "Reconsume later works properly" {

            let description = "Reconsume later works properly"

            description |> logTestStart

            let config = getTestConfig()
            let producerName = "reconsumeProducer"
            let consumerName = "reconsumeConsumer"

            let! (producer : IProducer<byte[]>) =
                createProducer()
                    .ProducerName(producerName)
                    .Topic(config.TopicName)
                    .EnableBatching(false)
                    .CreateAsync()

            let! (consumer : IConsumer<byte[]>) =
                createConsumer()
                    .ConsumerName(consumerName)
                    .Topic(config.TopicName)
                    .SubscriptionName(config.SubscriptionName)
                    .EnableRetry(true)
                    .SubscribeAsync()
         
            let! msgId = producer.SendAsync([| 0uy; 1uy; 0uy |]) 
            let! (msg1 : Message<byte[]>) = consumer.ReceiveAsync() 
            do! consumer.ReconsumeLaterAsync(msg1, %(DateTime.UtcNow.AddSeconds(1.0) |> convertToMsTimestamp)) 
            let! (msg2 : Message<byte[]>) = consumer.ReceiveAsync() 

            Expect.equal "" msgId msg1.MessageId
            Expect.equal "" (msg1.GetValue() |> Array.toList) (msg2.GetValue() |> Array.toList)

            description |> logTestEnd
        }
    ]
