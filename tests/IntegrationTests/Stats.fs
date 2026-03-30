module Pulsar.Client.IntegrationTests.Stats

open System
open System.Text.Json
open Expecto

open System.Threading.Tasks
open System.Reflection
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =
    testList "Stats" [
        testTask "Consumer and Producer stats" {
            let client = getStatsClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "Stats"

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages consumerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages consumerName
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)
            do! Task.Delay 1000
            let! producerStats = producer.GetStats()
            do! Task.Delay 1000
            let! consumerStats = consumer.GetStats()

            let numberOfMessages = int64 numberOfMessages
            Expect.equal producerStats.TotalMsgsSent numberOfMessages "The producStats.TotalMsgsSent is not equal to numberOfMessages"
            Expect.equal producerStats.TotalAcksReceived numberOfMessages "The producStats.TotalAcksReceived is not equal to numberOfMessages"
            Expect.isLessThan producerStats.SendLatencyMin Double.MaxValue "The producStats.SendLatencyMin should be less Double.MaxValue"
            Expect.isGreaterThan producerStats.SendLatencyMax Double.MinValue "The producStats.SendLatencyMax should be more Double.MinValue"
            Expect.isGreaterThan producerStats.SendLatencyAverage 0.0 "The producStats.SendLatencyAverage should be more 0"

            Expect.equal consumerStats.TotalMsgsReceived numberOfMessages "The consumStats.TotalMsgsReceived is not equal to numberOfMessages"
            Expect.equal consumerStats.TotalAcksSent numberOfMessages "The consumStats.TotalAcksSent is not equal to numberOfMessages"
        }

        testTask "Broker topic stats should expose prefixed client version" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync()

            let! _ = producer.SendAsync([| 1uy |])
            do! Task.Delay 3000

            let url = $"{pulsarHttpAddress}/admin/v2/persistent/{topicName}/stats"
            let! response : string = commonHttpClient.GetStringAsync(url)
            let json = JsonDocument.Parse(response)
            let publishers = json.RootElement.GetProperty("publishers")
            let clientVersion = publishers[0].GetProperty("clientVersion").GetString()
            let expectedVersion = $"pulsar-client-dotnet-{Assembly.GetAssembly(typeof<PulsarClient>).GetName().Version.ToString()}"

            Expect.equal clientVersion expectedVersion "Expected broker stats to report the prefixed .NET client version."
        }
    ]