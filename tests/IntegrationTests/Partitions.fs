module Pulsar.Client.IntegrationTests.Partitions

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

    testList "partitions" [

        testAsync "Single producer and single consumer with 3 partitions" {

            Log.Debug("Started Single producer and single consumer with 3 partitions")
            let client = getClient()
            let topicName = "public/default/partitioned"
            let consumerName = "PartitionedConsumer"
            let producerName = "PartitionedProducer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ConsumerName(consumerName)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .SubscribeAsync() |> Async.AwaitTask


            let messages = generateMessages 100 producerName

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

            Log.Debug("Finished Single producer and single consumer with 3 partitions")

        }

        testAsync "Two producers and two consumers with 2 partitions" {

            Log.Debug("Started Two producers and two consumers with 2 partitions")
            let client = getClient()
            let topicName = "public/default/partitioned2"
            let consumerName1 = "PartitionedConsumer1"
            let consumerName2 = "PartitionedConsumer2"
            let producerName1 = "PartitionedProducer1"
            let producerName2 = "PartitionedProducer2"

            let! producer1 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName1)
                    .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                    .CreateAsync() |> Async.AwaitTask

            let! producer2 =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName2)
                    .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName1)
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Shared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName2)
                    .SubscribeAsync() |> Async.AwaitTask


            let messages1 = generateMessages 50 producerName1
            let messages2 = generateMessages 50 producerName2
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

            let getStream (consumer: IConsumer) =
                asyncSeq {
                    while not consumer.HasReachedEndOfTopic do
                        let! message = (consumer.ReceiveAsync() |> Async.AwaitTask)
                        do! consumer.AcknowledgeAsync(message.MessageId) |> Async.AwaitTask
                        yield message
                }

            let stream1 = getStream consumer1
            let stream2 = getStream consumer2
            let resultStream = AsyncSeq.mergeAll([stream1;stream2])
            let enumerator = resultStream.GetEnumerator()


            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for i in [1..100] do
                            let! message = enumerator.MoveNext()
                            let received = Encoding.UTF8.GetString(message.Value.Payload)
                            Log.Debug("Some consumer received {1}", received)
                            if allMessages |> Array.contains received |> not then
                                failwith <| sprintf "Received unexpected message '%s'" received
                    }:> Task)

            do! Task.WhenAll(producerTask1, producerTask2, consumerTask) |> Async.AwaitTask
            do! Async.Sleep(110) // wait for acks

            Log.Debug("Finished Two producers and two consumers with 2 partitions")
        }

        testAsync "Messages with same key always go to the same consumer" {

            Log.Debug("Started Messages with same key always go to the same consumer")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName1 = "PartitionedConsumer1"
            let consumerName2 = "PartitionedConsumer2"
            let producerName = "PartitionedProducer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.KeyShared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName1)
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.KeyShared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName2)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let firstKey = "111111"
                        let secondKey = "444444"
                        let getMessageBuilder key i =
                            MessageBuilder(Encoding.UTF8.GetBytes(key + "Hello" + i), key)
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "0")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "0")
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "1")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "1")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "2")
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "2")
                        ()
                    }:> Task)

            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        let! msg1 = consumer1.ReceiveAsync()
                        let! msg2 = consumer1.ReceiveAsync()
                        let! msg3 = consumer1.ReceiveAsync()
                        let prefix = (string msg1.MessageKey).Substring(0,6)
                        [msg1;msg2;msg3]
                            |> List.iteri
                            (fun i elem ->
                                let strKey = string elem.MessageKey
                                let message = Encoding.UTF8.GetString(elem.Payload)
                                if (strKey = prefix && message.StartsWith(prefix) && message.EndsWith(i.ToString())) |> not then
                                    failwith <| sprintf "Incorrect key %s prefix %s consumer %s" strKey prefix consumerName1
                                else
                                    ()
                            )
                        Log.Debug("consumer1Task finished")
                    }:> Task)

            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        let! msg1 = consumer2.ReceiveAsync()
                        let! msg2 = consumer2.ReceiveAsync()
                        let! msg3 = consumer2.ReceiveAsync()
                        let prefix = (string msg1.MessageKey).Substring(0,6)
                        [msg1;msg2;msg3]
                            |> List.iteri
                            (fun i elem ->
                                let strKey = string elem.MessageKey
                                let message = Encoding.UTF8.GetString(elem.Payload)
                                if (strKey = prefix && message.StartsWith(prefix) && message.EndsWith(i.ToString())) |> not then
                                    failwith <| sprintf "Incorrect key %s prefix %s consumer %s" strKey prefix consumerName2
                                else
                                    ()
                            )
                        Log.Debug("consumer2Task finished")
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) |> Async.AwaitTask

            Log.Debug("Finished Messages with same key always go to the same consumer")
        }
    ]
