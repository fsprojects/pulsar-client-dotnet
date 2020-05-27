module Pulsar.Client.IntegrationTests.BackPressure

open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.IntegrationTests.Common
open System.Threading.Tasks
open System

[<Tests>]
let tests =

    testList "schema" [

        testAsync "Default backpressure throws" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MaxPendingMessages(1)
                    .CreateAsync() |> Async.AwaitTask
            Expect.throwsT2<ProducerQueueIsFullError> (fun () ->
                    [| producer.SendAsync([|0uy|]); producer.SendAsync([|1uy|]) |]
                    |> Task.WhenAll
                    |> Task.WaitAll
                )
                |> ignore
        }
        
        testAsync "Default backpressure throws for sendandforget" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MaxPendingMessages(1)
                    .CreateAsync() |> Async.AwaitTask
            Expect.throwsT2<ProducerQueueIsFullError> (fun () ->
                    [| producer.SendAndForgetAsync([|0uy|]); producer.SendAndForgetAsync([|1uy|]) |]
                    |> Task.WhenAll
                    |> Task.WaitAll
                )
                |> ignore
        }
        
        testAsync "Blocking backpressure doesn't throw" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .BlockIfQueueFull(true)
                    .MaxPendingMessages(1)
                    .CreateAsync() |> Async.AwaitTask
            let! messageIds =
                    [| producer.SendAsync([|0uy|]); producer.SendAsync([|1uy|]); producer.SendAsync([|2uy|]) |]
                    |> Task.WhenAll
                    |> Async.AwaitTask
            Expect.isLessThan "" (messageIds.[0], messageIds.[1])
            Expect.isLessThan "" (messageIds.[1], messageIds.[2])
        }
        
        testAsync "Blocking backpressure doesn't throw for sendAndForget" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .BlockIfQueueFull(true)
                    .MaxPendingMessages(1)
                    .CreateAsync() |> Async.AwaitTask
            let! _ =
                    [| producer.SendAndForgetAsync([|0uy|]); producer.SendAndForgetAsync([|1uy|]); producer.SendAndForgetAsync([|2uy|]) |]
                    |> Task.WhenAll
                    |> Async.AwaitTask
            ()
        }
    ]