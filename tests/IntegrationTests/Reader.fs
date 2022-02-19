module Pulsar.Client.IntegrationTests.Reader

open System
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Api

open System.Text
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open FSharp.Control
open FSharp.UMX

[<Tests>]
let tests =

    let readerLoopRead (reader: IReader<byte[]>) =
        task {
            let! hasSomeMessages = reader.HasMessageAvailableAsync()
            let mutable continueLooping = hasSomeMessages
            let resizeArray = ResizeArray<Message<byte[]>>()
            while continueLooping do
                let! msg = reader.ReadNextAsync()
                let received = Encoding.UTF8.GetString(msg.Data)
                Log.Debug("received {0}", received)
                resizeArray.Add(msg)
                let! hasNewMessage = reader.HasMessageAvailableAsync()
                continueLooping <- hasNewMessage
            return resizeArray
        }

    let basicReaderCheck batching =
        task {
            Log.Debug("Started Reader basic configuration works fine batching: " + batching.ToString())
            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let producerName = "readerProducer"
            let readerName = "basicReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            do! produceMessages producer numberOfMessages producerName

            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
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
            let producerName = "readerProducer"
            let readerName = "producerIdReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            do! produceMessages producer numberOfMessages producerName
            do! producer.DisposeAsync()

            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName + "1")
                    .StartMessageId(MessageId.Earliest)
                    .CreateAsync()
            let! result = readerLoopRead reader
            Expect.equal "" numberOfMessages result.Count
            do! reader.DisposeAsync()

            let! reader2 =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName + "2")
                    .StartMessageId(result.[0].MessageId)
                    .CreateAsync()
            let! result2 = readerLoopRead reader2
            Expect.equal "" (numberOfMessages-1) result2.Count

            let! reader3 =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName + "3")
                    .StartMessageId(result.[0].MessageId)
                    .StartMessageIdInclusive()
                    .CreateAsync() |> Async.AwaitTask
            let! result3 = readerLoopRead reader3
            Expect.equal "" numberOfMessages result3.Count

            do! reader2.DisposeAsync()
            do! reader3.DisposeAsync()

            Log.Debug("Finished Muliple readers non-batching configuration works fine batching: " + batching.ToString())
        }

    let checkReadingFromProducerMessageId batching =
        task {
            Log.Debug("Started Check reading from producer messageId. Batching: " + batching.ToString())

            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let producerName = "readerProducer"
            let readerName = "producerIdReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            let! msgId1 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #1 Sent from %s on %s" producerName (DateTime.Now.ToLongTimeString()) ))
            Log.Debug("msgId1 is {0}", msgId1)
            let! msgId2 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #2 Sent from %s on %s" producerName (DateTime.Now.ToLongTimeString()) ))
            Log.Debug("msgId2 is {0}", msgId2)
            do! producer.DisposeAsync()

            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
                    .StartMessageId(msgId1)
                    .CreateAsync()

            let! result = readerLoopRead reader
            Expect.equal "" 1 result.Count
            do! reader.DisposeAsync()

            Log.Debug("Finished Check reading from producer messageId. Batching: " + batching.ToString())
        }

    let checkReadingFromLastMessageId batching =
        task {
            Log.Debug("Started Check reading from last messageId. Batching: " + batching.ToString())

            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let producerName = "readerProducer"
            let readerName = "producerIdReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            let! msgId1 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #1 Sent from %s on %s" producerName (DateTime.Now.ToLongTimeString()) ))
            Log.Debug("msgId1 is {0}", msgId1)
            let! msgId2 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #2 Sent from %s on %s" producerName (DateTime.Now.ToLongTimeString()) ))
            Log.Debug("msgId2 is {0}", msgId2)
            do! producer.DisposeAsync()

            let! exclusiveReader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
                    .StartMessageId(msgId2)
                    .CreateAsync()
            let! exclusiveResult = exclusiveReader.HasMessageAvailableAsync()
            
            let! inclusiveReader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
                    .StartMessageId(msgId2)
                    .StartMessageIdInclusive()
                    .CreateAsync()
            let! inclusiveResult = inclusiveReader.HasMessageAvailableAsync()
            
            do! exclusiveReader.DisposeAsync()
            do! inclusiveReader.DisposeAsync()
            
            Expect.isFalse "exclusive" exclusiveResult
            Expect.isTrue "inclusive" inclusiveResult
            
            Log.Debug("Finished Check reading from last messageId. Batching: " + batching.ToString())
        }
    
    let checkReadingFromRollback batching =
        task {
            Log.Debug("Started Check StartMessageFromRollbackDuration. Batching: " + batching.ToString())

            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let producerName = "readerRollbackDurationProducer"
            let readerName = "rollbackDurationReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            let! _ = producer.SendAsync("Hello world1" |> Encoding.UTF8.GetBytes)
            do! Async.Sleep(500)
            let! _ = producer.SendAsync("Hello world2" |> Encoding.UTF8.GetBytes)
            do! Async.Sleep(500)
            let! _ = producer.SendAsync("Hello world3" |> Encoding.UTF8.GetBytes)
            do! Async.Sleep(500)

            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
                    .StartMessageFromRollbackDuration(TimeSpan.FromSeconds(20.0))
                    .CreateAsync()

            let! result = readerLoopRead reader
            Expect.equal "" 3 result.Count
            Log.Debug("Finished StartMessageFromRollbackDuration. Batching: " + batching.ToString())
        }
        
    let checkReadingFromFuture batching =
        task {
            Log.Debug("Started CheckReadingFromFuture. Batching: " + batching.ToString())

            let client = getClient()
            let topicName = "public/retention/" + Guid.NewGuid().ToString("N")
            let producerName = "futureProducer"
            let readerName = "futureReader"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(batching)
                    .CreateAsync()

            let! _ = producer.SendAsync("Hello world1" |> Encoding.UTF8.GetBytes)
            do! Task.Delay(4000)

            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(readerName)
                    .StartMessageId(MessageId.Latest)
                    .CreateAsync()

            do! reader.SeekAsync(%(DateTimeOffset.UtcNow.AddSeconds(-1.0).ToUnixTimeMilliseconds()))
            let! hasSomeMessages = reader.HasMessageAvailableAsync()
            Expect.equal "" false hasSomeMessages
            Log.Debug("Finished CheckReadingFromFuture. Batching: " + batching.ToString())
        }

    

    testList "Reader" [

        testTask "Reader non-batching configuration works fine" {
            do! basicReaderCheck false 
        }

        testTask "Reader batching configuration works fine" {
            do! basicReaderCheck true 
        }

        testTask "Muliple readers non-batching configuration works fine" {
            do! checkMultipleReaders false 
        }

        testTask "Muliple readers batching configuration works fine" {
            do! checkMultipleReaders true 
        }

        testTask "Check reading from producer messageId without batching" {
            do! checkReadingFromProducerMessageId false 
        }

        testTask "Check reading from producer messageId with batching" {
            do! checkReadingFromProducerMessageId true 
        }
        
        testTask "Check reading from last messageId without batching" {
            do! checkReadingFromLastMessageId false 
        }
        
        testTask "Check reading from last messageId with batching" {
            do! checkReadingFromLastMessageId true 
        }
        
        testTask "Check StartMessageFromRollbackDuration without batching" {
            do! checkReadingFromRollback false 
        }
        
        testTask "Check StartMessageFromRollbackDuration with batching" {
            do! checkReadingFromRollback true 
        }
        
        // uncomment when https://github.com/apache/pulsar/issues/10515 is ready
        ptestTask "Check StartMessageFromFuturePoint without batching" {
            do! checkReadingFromFuture false 
        }
        
        // uncomment when https://github.com/apache/pulsar/issues/10515 is ready
        ptestTask "Check StartMessageFromFuturePoint with batching" {
            do! checkReadingFromFuture true 
        }
    ]