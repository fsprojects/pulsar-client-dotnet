module Pulsar.Client.IntegrationTests.Reader

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
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX
open FSharp.Control

[<Tests>]
let tests =

    let readerLoopRead (reader: Reader) =
        task {
            let! hasSomeMessages = reader.HasMessageAvailableAsync()
            let mutable continueLooping = hasSomeMessages
            let resizeArray = ResizeArray<Message>()
            while continueLooping do
                let! msg = reader.ReadNextAsync()
                let received = Encoding.UTF8.GetString(msg.Payload)
                Log.Debug("received {0}", received)
                resizeArray.Add(msg)
                let! hasNewMessage = reader.HasMessageAvailableAsync()
                continueLooping <- hasNewMessage
            return resizeArray
        }

    let bascisReaderCheck batching =
        task {
            Log.Debug("Started Reader basic configuration works fine batching: " + batching.ToString())
            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let producerName = "nonBatchReaderProducer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            do! produceMessages producer numberOfMessages producerName

            let! reader =
                ReaderBuilder(client)
                    .Topic(topicName)
                    .StartMessageId(MessageId.Earliest)
                    .CreateAsync()

            let! result = readerLoopRead reader
            Expect.equal "" numberOfMessages result.Count
            do! reader.SeekAsync(result.[0].MessageId)
            let! result2 = readerLoopRead reader
            Expect.equal "" (numberOfMessages - 1) result2.Count
            Log.Debug("Finished Reader basic configuration works fine batching: " + batching.ToString())
        }

    let checkMultipleReaders batching =
        task {
            Log.Debug("Started Muliple readers non-batching configuration works fine batching: " + batching.ToString())

            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let producerName = "nonBatchReaderProducer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            do! produceMessages producer numberOfMessages producerName
            do! producer.CloseAsync()

            let! reader =
                ReaderBuilder(client)
                    .Topic(topicName)
                    .StartMessageId(MessageId.Earliest)
                    .CreateAsync()
            let! result = readerLoopRead reader
            Expect.equal "" numberOfMessages result.Count
            do! reader.CloseAsync()

            let! reader2 =
                ReaderBuilder(client)
                    .Topic(topicName)
                    .StartMessageId(result.[0].MessageId)
                    .CreateAsync()
            let! result2 = readerLoopRead reader2
            Expect.equal "" (numberOfMessages-1) result2.Count

            let! reader3 =
                ReaderBuilder(client)
                    .Topic(topicName)
                    .StartMessageId(result.[0].MessageId)
                    .StartMessageIdInclusive(true)
                    .CreateAsync() |> Async.AwaitTask
            let! result3 = readerLoopRead reader3
            Expect.equal "" numberOfMessages result3.Count

            do! reader2.CloseAsync()
            do! reader3.CloseAsync()

            Log.Debug("Finished Muliple readers non-batching configuration works fine batching: " + batching.ToString())
        }


    testList "reader" [

        testAsync "Reader non-batching configuration works fine" {
            do! bascisReaderCheck false |> Async.AwaitTask
        }

        testAsync "Reader batching configuration works fine" {
            do! bascisReaderCheck true |> Async.AwaitTask
        }

        testAsync "Muliple readers non-batching configuration works fine" {
            do! checkMultipleReaders false |> Async.AwaitTask
        }

        testAsync "Muliple readers batching configuration works fine" {
            do! checkMultipleReaders true |> Async.AwaitTask
        }
    ]