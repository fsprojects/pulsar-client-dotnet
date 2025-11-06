module Pulsar.Client.IntegrationTests.Flush

open System
open System.Collections.Generic
open System.Text
open System.Threading
open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common


let testMessageOrderAndDuplicates (messageSet: HashSet<string>) (receivedMessage: string) (expectedMessage: string) =
    if messageSet.Contains(receivedMessage) then
        failwith $"Duplicate message received: {receivedMessage}"
    messageSet.Add(receivedMessage) |> ignore
    if receivedMessage <> expectedMessage then
        failwith $"Incorrect message order. Expected: {expectedMessage}, Received: {receivedMessage}"

[<Tests>]
let tests =

    testList "Flush" [

        testTask "Flush with batch enabled" {
            Log.Debug("Started Flush with batch enabled")
            let client = getClient()
            let topicName = "persistent://public/default/test-flush-batch-enabled-" + Guid.NewGuid().ToString("N")
        
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("my-subscriber-name")
                    .SubscribeAsync()
        
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchingMaxPublishDelay(TimeSpan.FromHours(1.0))
                    .BatchingMaxMessages(10000)
                    .CreateAsync()
        
            // Send 10 messages asynchronously without waiting
            for i in 0..9 do
                let message = $"my-message-{i}"
                producer.SendAsync(Encoding.UTF8.GetBytes(message)) |> ignore
        
            // Flush to ensure all messages are sent and acknowledged
            do! producer.FlushAsync()
        
            // Dispose producer
            do! (producer :> IAsyncDisposable).DisposeAsync().AsTask()
        
            // Receive and verify messages
            let messageSet = HashSet<string>()
            let cts = new CancellationTokenSource(TimeSpan.FromSeconds(5.0))
        
            for i in 0..9 do
                let! (msg : Message<byte[]>) = consumer.ReceiveAsync(cts.Token)
                let receivedMessage = Encoding.UTF8.GetString(msg.GetValue())
                Log.Debug("Received message: [{0}]", receivedMessage)
                let expectedMessage = $"my-message-{i}"
                testMessageOrderAndDuplicates messageSet receivedMessage expectedMessage
        
            do! (consumer :> IAsyncDisposable).DisposeAsync().AsTask()
        
            Log.Debug("Finished Started Flush with batch enabled")
        }

        testTask "Flush with batch disabled" {
            Log.Debug("Started Flush with batch disabled")
            let client = getClient()
            let topicName = "persistent://public/default/test-flush-batch-disabled-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("my-subscriber-name")
                    .SubscribeAsync()

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            // Send 10 messages asynchronously without waiting
            for i in 0..9 do
                let message = $"my-message-{i}"
                producer.SendAsync(Encoding.UTF8.GetBytes(message)) |> ignore

            // Flush to ensure all messages are sent and acknowledged
            do! producer.FlushAsync()

            // Dispose producer
            do! (producer :> IAsyncDisposable).DisposeAsync().AsTask()

            // Receive and verify messages
            let messageSet = HashSet<string>()
            let cts = new CancellationTokenSource(TimeSpan.FromSeconds(5.0))

            for i in 0..9 do
                let! (msg : Message<byte[]>) = consumer.ReceiveAsync(cts.Token)
                let receivedMessage = Encoding.UTF8.GetString(msg.GetValue())
                Log.Debug("Received message: [{0}]", receivedMessage)
                let expectedMessage = $"my-message-{i}"
                testMessageOrderAndDuplicates messageSet receivedMessage expectedMessage

            do! (consumer :> IAsyncDisposable).DisposeAsync().AsTask()

            Log.Debug("Finished Flush with batch disabled")
        }
    ]

