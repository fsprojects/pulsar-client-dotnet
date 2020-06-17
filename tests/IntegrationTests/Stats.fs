module Pulsar.Client.IntegrationTests.Stats

open System
open Expecto
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =
    testList "Stats" [
        testAsync "Consumer and Producer stats" {
            let client = getStatsClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "Stats"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

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
              
            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            do! Async.Sleep 1000
            let! producerStats = producer.GetStatsAsync() |> Async.AwaitTask
            do! Async.Sleep 1000
            let! consumerStats = consumer.GetStatsAsync() |> Async.AwaitTask
            
            let numberOfMessages = int64 numberOfMessages
            Expect.equal producerStats.TotalMsgsSent numberOfMessages "The producStats.TotalMsgsSent is not equal to numberOfMessages" 
            Expect.equal producerStats.TotalAcksReceived numberOfMessages "The producStats.TotalAcksReceived is not equal to numberOfMessages"
            Expect.isLessThan producerStats.SendLatencyMin Double.MaxValue "The producStats.SendLatencyMin should be less Double.MaxValue"
            Expect.isGreaterThan producerStats.SendLatencyMax Double.MinValue "The producStats.SendLatencyMax should be more Double.MinValue"
            Expect.isGreaterThan producerStats.SendLatencyAverage 0.0 "The producStats.SendLatencyAverage should be more 0"
            
            Expect.equal consumerStats.TotalMsgsReceived numberOfMessages "The consumStats.TotalMsgsReceived is not equal to numberOfMessages"
            Expect.equal consumerStats.TotalAcksSent numberOfMessages "The consumStats.TotalAcksSent is not equal to numberOfMessages"
        }
    ]