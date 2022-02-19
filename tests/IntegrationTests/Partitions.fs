module Pulsar.Client.IntegrationTests.Partitions

open System
open System.Collections.Generic
open System.Diagnostics
open System.Text.Json
open Expecto
open Expecto.Flip
open Pulsar.Client.Api

open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Serilog
open Pulsar.Client.IntegrationTests.Common
open FSharp.Control
open FSharp.UMX

[<CLIMutable>]
type SimpleRecord =
    {
        Name: string
        Age: int
    }
    
[<Tests>]
let tests =

    testList "Partitions" [

        testTask "Single producer and single consumer with 3 partitions" {

            Log.Debug("Started Single producer and single consumer with 3 partitions")
            let client = getClient()
            let topicName = "public/default/partitioned"
            let consumerName = "PartitionedConsumer"
            let producerName = "PartitionedProducer"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() 

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync() 

            let messages = generateMessages 10 producerName

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

            do! Task.WhenAll(producerTask, consumerTask) 
            do! Task.Delay(110) // wait for acks

            Log.Debug("Finished Single producer and single consumer with 3 partitions")

        }

        // make active after https://github.com/apache/pulsar/issues/5877 is resolved
        testTask "Two producers and two consumers with 2 partitions" {

            Log.Debug("Started Two producers and two consumers with 2 partitions")
            let client = getClient()
            let topicName = "public/default/partitioned2"
            let consumerName1 = "PartitionedConsumer1"
            let consumerName2 = "PartitionedConsumer2"
            let producerName1 = "PartitionedProducer1"
            let producerName2 = "PartitionedProducer2"

            let! producer1 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName1)
                    .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                    .CreateAsync() 

            let! producer2 =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName2)
                    .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                    .CreateAsync() 

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName1)
                    .SubscribeAsync() 

            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName2)
                    .SubscribeAsync() 

            let messages1 = generateMessages 5 producerName1
            let messages2 = generateMessages 5 producerName2
            let allMessages =  [| yield! messages1; yield! messages2 |]

            let producerTask1 =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer1 messages1
                    }:> Task)

            let producerTask2 =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer2 messages2
                    }:> Task)

            let getStream (consumer: IConsumer<'T>) =
                fun () ->
                    task {
                        let! message = (consumer.ReceiveAsync() |> Async.AwaitTask)
                        do! consumer.AcknowledgeAsync(message.MessageId) |> Async.AwaitTask
                        return message
                    }

            let stream1 = getStream consumer1
            let stream2 = getStream consumer2
            let resultStream = TaskSeq([stream1;stream2])

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in [1..10] do
                            let! message = resultStream.Next()
                            let received = Encoding.UTF8.GetString(message.Data)
                            Log.Debug("Some consumer received {1}", received)
                            if allMessages |> Array.contains received |> not then
                                failwith <| sprintf "Received unexpected message '%s'" received
                    }:> Task)

            do! Task.WhenAll(producerTask1, producerTask2, consumerTask) 
            do! Task.Delay(110) // wait for acks

            Log.Debug("Finished Two producers and two consumers with 2 partitions")
        }
        
        testTask "Batch read works with partitions" {

            Log.Debug("Started Batch read works with partitions")
            let client = getClient()
            let topicName = "public/default/partitioned3"
            let consumerName = "PartitionedConsumerBatchRead"
            let producerName = "PartitionedProducerBatchRead"
            let batchTimeout = TimeSpan.FromSeconds(2.0)
            let numberOfMessages = 10
            let batchSize = 8

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .BatchReceivePolicy(BatchReceivePolicy(batchSize, -1L, batchTimeout))
                    .SubscribeAsync() 

            let messages = generateMessages numberOfMessages producerName

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer messages
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let sw = Stopwatch()
                        sw.Start()
                        let! messagesBatch = consumer.BatchReceiveAsync()
                        let firstBatchTime = sw.Elapsed
                        if firstBatchTime > TimeSpan.FromSeconds(0.5) then
                                failwith <| sprintf "Too long to receive first batch consumer %s passed %f ms" consumerName firstBatchTime.TotalMilliseconds
                        if messagesBatch.Count <> batchSize then
                            failwith <| sprintf "Wrong number of messages received %i consumer %s" messagesBatch.Count consumerName
                        for message in messagesBatch do    
                            let received = Encoding.UTF8.GetString(message.Data)                        
                            Log.Debug("{0} received {1}", consumerName, received)
                            if messages |> Array.contains received |> not then
                                failwith <| sprintf "Incorrect message received %s consumer %s" received consumerName
                        do! consumer.AcknowledgeAsync(messagesBatch)
                        sw.Restart()
                        let! messagesBatch2 = consumer.BatchReceiveAsync()
                        let secondBatchTime = sw.Elapsed
                        if secondBatchTime < (batchTimeout - TimeSpan.FromMilliseconds(15.0)) then
                                failwith <| sprintf "Too fast to get second batch consumer %s passed %f ms" consumerName secondBatchTime.TotalMilliseconds
                        if messagesBatch2.Count <> (numberOfMessages - batchSize) then
                            failwith <| sprintf "Wrong number of messages2 received %i consumer %s" messagesBatch2.Count consumerName
                        for message in messagesBatch2 do    
                            let received = Encoding.UTF8.GetString(message.Data)                        
                            Log.Debug("{0} received {1}", consumerName, received)
                            if messages |> Array.contains received |> not then
                                failwith <| sprintf "Incorrect message received %s consumer %s" received consumerName
                        do! consumer.AcknowledgeAsync(messagesBatch2)
                            
                        
                    } :> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
            do! Task.Delay(110) // wait for acks

            Log.Debug("Finished Batch read works with partitions")

        }
        
        testTask "Auto schema works fine with partitioned topic" {

            Log.Debug("Start Auto schema works fine with partitioned topic")
            let client = getClient()
            let topicName1 = "public/default/partitioned4"
            let topicName2 = "public/default/partitioned5"

            let! (producer : IProducer<SimpleRecord>) =
                client.NewProducer(Schema.JSON<SimpleRecord>())
                    .Topic(topicName1)
                    .ProducerName("autoPartitionedNormal")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<GenericRecord>) =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName1)
                    .ConsumerName("autoPartitionedConsume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let! (consumer2 : IConsumer<SimpleRecord>) =
                client.NewConsumer(Schema.JSON<SimpleRecord>())
                    .Topic(topicName2)
                    .ConsumerName("autoNormal")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() 
            
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName2)
                    .ProducerName("autoProduce")
                    .EnableBatching(false)
                    .CreateAsync() 
            
            let messages = [|
                    for i in [1..10] do
                        yield { Name = "Name"; Age = i }
                |]
            
            for message in messages do
                let! _ = producer.SendAsync(message) 
                ()

            let firstDict =
                messages
                |> Seq.map (fun msg -> msg.Age, msg)
                |> dict
                |> Dictionary
            for i in [1..10] do
                let! (msg1 : Message<GenericRecord>) = consumer.ReceiveAsync() 
                do! consumer.AcknowledgeAsync msg1.MessageId 
                let genericRecord = msg1.GetValue()
                Expect.equal "" "Name" (genericRecord.GetField("Name") |> unbox)
                firstDict.Remove(genericRecord.GetField("Age") |> unbox<float> |> int) |> ignore
                let! _ = producer2.SendAsync(msg1.Data) 
                ()
            Expect.isEmpty "firstDict" firstDict
            
            let secondDict =
                messages
                |> Seq.map (fun msg -> msg.Age, msg)
                |> dict
                |> Dictionary
            for i in [1..10] do
                let! (msg1 : Message<SimpleRecord>) = consumer2.ReceiveAsync() 
                do! consumer2.AcknowledgeAsync msg1.MessageId 
                let jsonRecord = msg1.GetValue()
                Expect.equal "" "Name" jsonRecord.Name
                secondDict.Remove(jsonRecord.Age) |> ignore
            Expect.isEmpty "secondDict" secondDict            
            
            Log.Debug("Finished Auto schema works fine with partitioned topic")
        }        
        
        testTask "Deduplication works with partitions" {

            Log.Debug("Started Deduplication works with partitions")

            let messagesCount = 12
            let client = getClient()
            let topicName = "public/deduplication/partitioned"
            let name = "deduplicationPartitions" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() 
                    
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(name)
                    .BatchReceivePolicy(BatchReceivePolicy(-1, 10485760L, TimeSpan.FromSeconds(1.0)))
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = producer.NewMessage(payload, i.ToString(), sequenceId = Nullable(%(int64 i))  )
                let! m1 = producer.SendAsync(message) 
                Log.Logger.Debug("Sent {0} to {1}", i, m1.Partition)
                let! m2 = producer.SendAsync(message) 
                Log.Logger.Debug("Sent {0} to {1}", i, m2.Partition)
                ()            
            
            do! producer.DisposeAsync().AsTask()             
            
            let! (newProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .InitialSequenceId(%0L)
                    .CreateAsync() 
                    
            for i in 1..messagesCount do
                let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producer.Name (DateTime.Now.ToLongTimeString()) )
                let message = newProducer.NewMessage(payload, i.ToString(), sequenceId = Nullable(%(int64 i))  )
                let! _ = newProducer.SendAndForgetAsync(message) 
                ()

            let! (messages : Messages<byte[]>) = consumer.BatchReceiveAsync() 
            
            Expect.equal "" messagesCount messages.Count
            
            do! consumer.AcknowledgeAsync(messages) 
            do! Task.Delay(110)
            do! consumer.DisposeAsync().AsTask() 
            
            Log.Debug("Finished Deduplication works with partitions")
        }

    ]
