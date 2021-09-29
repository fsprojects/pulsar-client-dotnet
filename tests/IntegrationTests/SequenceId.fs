module Pulsar.Client.IntegrationTests.SequenceId

open System
open System.Text
open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open System.Threading.Tasks

open Serilog
open FSharp.UMX

[<Tests>]
let tests =

    
    let client = getClient()

    testList "sequenceId" [

        testAsync "Set sequenceId explicitly for message" {

            Log.Debug("Started 'Set sequenceId explicitly for message'")

            let messagesCount = 10
            let sequenceIdStart = Random().Next()
            let getSequenceId i = %(sequenceIdStart + i |> int64)
            let topicName = "public/deduplication/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("explicitSeqid")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("explicitSeqid")
                    .SubscriptionName("sequence-id-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        for i in 1..messagesCount do
                            let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                            let sequenceId = Nullable<SequenceId>(getSequenceId i)
                            let message = producer.NewMessage(payload, sequenceId = sequenceId)
                            let! _ = producer.SendAsync(message)
                            ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in 1..messagesCount do
                            let expectedSequenceId = getSequenceId i
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync(message.MessageId)
                            if message.SequenceId <> expectedSequenceId then
                                failwith <| sprintf "Incorrect sequenceId. Expected '%i' but '%i'" expectedSequenceId message.SequenceId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished 'Set sequenceId explicitly for message'")
        }

        testAsync "GetLastSequenceId works" {

            Log.Debug("Started GetLastSequenceId works")

            let messagesCount = 10
            let topicName = "public/deduplication/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "explicitSeqid"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            for i in 0..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = producer.NewMessage(payload)
                let! _ = producer.SendAsync(message) |> Async.AwaitTask
                ()

            do! producer.DisposeAsync().AsTask() |> Async.AwaitTask
            
            let! newProducer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("explicitSeqid")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            Expect.equal "" newProducer.LastSequenceId %10L

            Log.Debug("Finished GetLastSequenceId works")
        }
        
        testAsync "Deduplication works for single messages" {

            Log.Debug("Started Deduplication works for single messages")

            let messagesCount = 10
            let topicName = "public/deduplication/topic-" + Guid.NewGuid().ToString("N")
            let name = "deduplicationCheckSingle"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("sequence-id-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = producer.NewMessage(payload, sequenceId = Nullable(%(int64 i))  )
                let! _ = producer.SendAsync(message) |> Async.AwaitTask
                let! _ = producer.SendAsync(message) |> Async.AwaitTask
                ()
            
            do! producer.DisposeAsync().AsTask() |> Async.AwaitTask
            
            let! newProducer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .InitialSequenceId(%0L)
                    .CreateAsync() |> Async.AwaitTask
                    
            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = newProducer.NewMessage(payload, sequenceId = Nullable(%(int64 i))  )
                let! _ = newProducer.SendAsync(message) |> Async.AwaitTask
                ()
                                
            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(name)
                    .StartMessageId(MessageId.Earliest)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! hasSomeMessages = reader.HasMessageAvailableAsync() |> Async.AwaitTask
            let mutable continueLooping = hasSomeMessages
            let mutable i = 0
            while continueLooping do
                i <- i + 1
                let! msg = reader.ReadNextAsync() |> Async.AwaitTask
                Expect.equal "" %(int64 i) msg.SequenceId
                let! hasNewMessage = reader.HasMessageAvailableAsync() |> Async.AwaitTask
                continueLooping <- hasNewMessage
            
            Expect.equal "" i messagesCount
            
            Log.Debug("Finished Deduplication works for single messages")
        }
        
        testAsync "Deduplication works for batch messages" {

            Log.Debug("Started Deduplication works for batch messages")

            let messagesCount = 10
            let topicName = "public/deduplication/topic-" + Guid.NewGuid().ToString("N")
            let name = "deduplicationCheckBatch"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(true)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("sequence-id-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = producer.NewMessage(payload, sequenceId = Nullable(%(int64 i))  )
                let! _ = producer.SendAsync(message) |> Async.AwaitTask
                let! _ = producer.SendAsync(message) |> Async.AwaitTask
                ()
            
            do! producer.DisposeAsync().AsTask() |> Async.AwaitTask
            
            let! newProducer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(true)
                    .InitialSequenceId(%0L)
                    .CreateAsync() |> Async.AwaitTask
                    
            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = newProducer.NewMessage(payload, sequenceId = Nullable(%(int64 i))  )
                let! _ = newProducer.SendAsync(message) |> Async.AwaitTask
                ()
                                
            let! reader =
                client.NewReader()
                    .Topic(topicName)
                    .ReaderName(name)
                    .StartMessageId(MessageId.Earliest)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! hasSomeMessages = reader.HasMessageAvailableAsync() |> Async.AwaitTask
            let mutable continueLooping = hasSomeMessages
            let mutable i = 0
            while continueLooping do
                i <- i + 1
                let! msg = reader.ReadNextAsync() |> Async.AwaitTask
                Expect.equal "" %(int64 i) msg.SequenceId
                let! hasNewMessage = reader.HasMessageAvailableAsync() |> Async.AwaitTask
                continueLooping <- hasNewMessage
            
            Expect.equal "" i messagesCount
            
            Log.Debug("Finished Deduplication works for batch messages")
        }
    
    ]