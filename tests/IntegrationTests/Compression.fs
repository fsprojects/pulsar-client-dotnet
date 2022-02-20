module Pulsar.Client.IntegrationTests.Compression

open System
open Expecto

open System.Threading.Tasks
open Serilog
open Pulsar.Client.IntegrationTests.Common
open Pulsar.Client.Common

[<Tests>]
let tests =

    let codecs =
        [|
            CompressionType.None
            CompressionType.LZ4
            CompressionType.Snappy
            CompressionType.ZLib
            CompressionType.ZStd
        |]

    let sendReceive enableBatching (compressionType : CompressionType) = task {

        let singleOrBatched = if enableBatching then "batched" else "single"

        Log.Debug("Started Send and receive {0} compressed message using '{1}'", singleOrBatched, compressionType)

        let client = getClient()
        let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

        let! producer =
            client.NewProducer()
                .Topic(topicName)
                .ProducerName("compression-single")
                .EnableBatching(enableBatching)
                .CompressionType(compressionType)
                .CreateAsync() 

        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .ConsumerName("compression-single")
                .SubscriptionName("test-subscription")
                .SubscribeAsync() 

        let sendMessages = if enableBatching then fastProduceMessages else produceMessages
        let messagesCount = 10

        let producerTask =
            Task.Run(fun () ->
                task {
                    do! sendMessages producer messagesCount "compression-producer"
                }:> Task)

        let consumerTask =
            Task.Run(fun () ->
                task {
                    do! consumeMessages consumer messagesCount "compression-consumer"
                }:> Task)

        do! Task.WhenAll(producerTask, consumerTask) 

        Log.Debug("Finished Send and receive {0} compressed message using '{0}'", singleOrBatched, compressionType)
    }

    let sendMessages enableBatching =
        codecs
        |> Seq.map (sendReceive enableBatching)
        |> Task.FromResult

    let sendNonBatchedMessages() = sendMessages false
    let sendBatchedMessages() = sendMessages true

    testList "Compression" [
        testTask "Send and receive single compressed message using all implemented compression codecs" {
            do! sendNonBatchedMessages()
        }

        testTask "Send and receive batched compressed message using all implemented compression codecs" {
            do! sendBatchedMessages()
        }
    ]
