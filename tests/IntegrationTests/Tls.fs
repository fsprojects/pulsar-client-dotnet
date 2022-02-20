module Pulsar.Client.IntegrationTests.Tls

#if !NOTLS

open System
open Expecto

open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common


[<Tests>]
let tests =
    testList "Tls" [
        testTask "Tls transport" {
            let client = getSslAdminClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
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
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                        do! consumer.DisposeAsync()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
        }

    ]
#endif
