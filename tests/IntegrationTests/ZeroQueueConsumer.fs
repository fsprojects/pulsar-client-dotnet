module Pulsar.Client.IntegrationTests.ZeroQueueConsumer

open System
open Expecto
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =
    ftestAsync "ZeroQueueConsumer" {
        let client = getClient()
        let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
        let numberOfMessages = 10
        let consumerName = "ZeroQueue"
        
        let! producer =
            client.NewProducer()
                .Topic(topicName)
                .EnableBatching(false)
                .CreateAsync() |> Async.AwaitTask
        
        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .ConsumerName(consumerName)
                .ReceiverQueueSize(0)
                .SubscriptionName("test-subscription")
                .SubscribeAsync() |> Async.AwaitTask

        let messages = generateMessages numberOfMessages consumerName
        
        let producerTask =
            Task.Run(fun () ->
                task {
                    do! producePredefinedMessages producer messages
                }:> Task)

        let consumerTask =
            Task.Run(fun () ->
                task {
                    do! consumeAndVerifyMessages consumer consumerName messages
                }:> Task)

        do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
        do! Async.Sleep(110) // wait for acks
    }