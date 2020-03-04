module Pulsar.Client.IntegrationTests.SequenceId

open System
open Expecto
open Pulsar.Client.IntegrationTests.Common
open Pulsar.Client.Api
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive
open Serilog

[<Tests>]
let tests =

    let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

    let client = getClient()

    let getProducer() =
        ProducerBuilder(client)
            .Topic(topicName)
            .ProducerName("sequence-id-producer")
            .EnableBatching(false)
            .CreateAsync() |> Async.AwaitTask

    let getConsumer() =
        ConsumerBuilder(client)
            .Topic(topicName)
            .ConsumerName("sequence-id-consumer")
            .SubscriptionName("sequence-id-subscription")
            .SubscribeAsync() |> Async.AwaitTask



    testList "sequenceId" [

        testAsync "Set sequenceId explicitly for message" {

            Log.Debug("Started 'Set sequenceId explicitly for message'")

            let messagesCount = 10
            let sequenceIdStart = Random().Next()
            let getSequenceId i = (sequenceIdStart + i) |> uint64

            let! producer = getProducer()
            let! consumer = getConsumer()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithSequenceId producer messagesCount getSequenceId
                        return ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithSequenceId consumer messagesCount getSequenceId
                        return ()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished 'Set sequenceId explicitly for message'")
        }

    ]