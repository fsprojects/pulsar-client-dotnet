module Pulsar.Client.IntegrationTests.Compression

open System
open Expecto
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
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

    let sendReceive enableBatching (compressionType : CompressionType) = async {

        let singleOrBatched = if enableBatching then "batched" else "single"

        Log.Debug("Started Send and receive {0} compressed message using '{1}'", singleOrBatched, compressionType)

        let client = getClient()
        let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

        let! producer =
            ProducerBuilder(client)
                .Topic(topicName)
                .ProducerName("compression-single")
                .EnableBatching(enableBatching)
                .CompressionType(compressionType)
                .CreateAsync() |> Async.AwaitTask

        let! consumer =
            ConsumerBuilder(client)
                .Topic(topicName)
                .ConsumerName("compression-single")
                .SubscriptionName("test-subscription")
                .SubscribeAsync() |> Async.AwaitTask

        let producerTask =
            Task.Run(fun () ->
                task {
                    do! produceMessages producer 1 "compression-single"
                }:> Task)

        let consumerTask =
            Task.Run(fun () ->
                task {
                    do! consumeMessages consumer 1 "compression-single"
                }:> Task)

        do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

        Log.Debug("Finished Send and receive {0} compressed message using '{0}'", singleOrBatched, compressionType)
    }

    let run enableBatching =
        codecs
        |> Seq.map (sendReceive enableBatching)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore

    testList "compression" [
        testAsync "Send and receive single compressed message using all implemented compression codecs" {
            run false
        }

        testAsync "Send and receive batched compressed message using all implemented compression codecs" {
            run true
        }
    ]
