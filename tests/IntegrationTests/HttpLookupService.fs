module Pulsar.Client.IntegrationTests.HttpLookupService

open System
open Expecto
open Expecto.Flip
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System.Collections.Generic

[<CLIMutable>]
type KeySchema =
    {
        Name: string
        Age: int
    }

[<CLIMutable>]
type ValueSchema =
    {
        Name: int
        Age: string
    }

[<Tests>]
let tests =

    testList "HttpLookupService" [

        testTask "HttpLookupService test basic functions" {
            Log.Debug("Started HttpLookupService test basic functions")
            let client = getHttpLookupClient()
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
            Log.Debug("Finished HttpLookupService test basic functions")
        }

        testTask "HttpLookupService test send/receive multiple messages" {
            Log.Debug("Started HttpLookupService test send/receive multiple messages")
            let client = getHttpLookupClient()
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
            Log.Debug("Finished HttpLookupService test send/receive multiple messages")
        }

        testTask "HttpLookupService test GetTopicsUnderNamespace function" {
            Log.Debug("Started HttpLookupService test GetTopicsUnderNamespace function")
            let subscriptionName = "testPulsar"
            let topicPattern = sprintf "persistent://public/default/%s-*" (Guid.NewGuid().ToString("N"))
            let topic1 = topicPattern.Replace("*", "1")
            let topic2 = topicPattern.Replace("*", "2")
            let client = getHttpLookupClient()
            let! (producer1 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topic1)
                    .CreateAsync()
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .TopicsPattern(topicPattern)
                    .PatternAutoDiscoveryPeriod(TimeSpan.FromSeconds(4.0))
                    .SubscriptionName(subscriptionName)
                    .SubscribeAsync()
            let send1 =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let msgStr = sprintf "Message #%i Sent to %s on %s" i topic1 (DateTime.Now.ToLongTimeString())
                            let! _ = producer1.SendAsync(msgStr |> Encoding.UTF8.GetBytes)
                            ()
                    } :> Task
                )
            let receiveAll =
                Task.Run(fun () ->
                        task {
                            for _ in [1..20] do
                                let! message = consumer.ReceiveAsync()
                                do! consumer.AcknowledgeAsync(message.MessageId)
                        } :> Task
                    )
            do! Task.WhenAll(send1)
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topic2)
                    .CreateAsync()
            do! Task.Delay(5000)
            let send2 =
                Task.Run(fun () ->
                    task {
                        for i in [1..10] do
                            let msgStr = sprintf "Message #%i Sent to %s on %s" i topic2 (DateTime.Now.ToLongTimeString())
                            let! _ = producer2.SendAsync(msgStr |> Encoding.UTF8.GetBytes)
                            ()
                    } :> Task
                )
            do! Task.WhenAll(send2, receiveAll)
            Log.Debug("Finished HttpLookupService test GetTopicsUnderNamespace function")
        }

        testTask "HttpLookupService test GetSchema function" {
            Log.Debug("Started HttpLookupService test GetSchema function")
            let client = getHttpLookupClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "autoProduceKeyValue"
            let! (producer : IProducer<KeyValuePair<KeySchema, ValueSchema>>) =
                client.NewProducer(Schema.KEY_VALUE<KeySchema, ValueSchema>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync()
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName)
                    .ProducerName(name + "Main")
                    .EnableBatching(false)
                    .CreateAsync()
            let! (consumer : IConsumer<KeyValuePair<KeySchema, ValueSchema>>) =
                client.NewConsumer(Schema.KEY_VALUE<KeySchema, ValueSchema>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
            let keyInput = { KeySchema.Name = "abc"; Age = 20 }
            let valueInput = { ValueSchema.Name = 20; Age = "abc" }
            let! _ = producer.SendAsync(KeyValuePair(keyInput, valueInput))
            let! (msg : Message<KeyValuePair<KeySchema, ValueSchema>>) = consumer.ReceiveAsync()
            do! consumer.AcknowledgeAsync msg.MessageId
            let (KeyValue(key, value)) = msg.GetValue()
            Expect.equal "" keyInput key
            Expect.equal "" valueInput value
            let! _ = producer2.SendAsync(msg.Data)
            let! (msg2 : Message<KeyValuePair<KeySchema, ValueSchema>>) = consumer.ReceiveAsync()
            do! consumer.AcknowledgeAsync msg2.MessageId
            let (KeyValue(key2, value2)) = msg2.GetValue()
            Expect.equal "" keyInput key2
            Expect.equal "" valueInput value2
            do! consumer.UnsubscribeAsync()
            Log.Debug("HttpLookupService test GetSchema function")
        }
    ]
