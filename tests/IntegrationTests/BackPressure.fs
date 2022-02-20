module Pulsar.Client.IntegrationTests.BackPressure

open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open System.Threading.Tasks
open System

[<Tests>]
let tests =

    testList "Backpressure" [

        testTask "Default backpressure throws" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MaxPendingMessages(1)
                    .CreateAsync()
                    
            Expect.throwsT2<ProducerQueueIsFullError> (fun () ->
                    [| producer.SendAsync([|0uy|]); producer.SendAsync([|1uy|]) |]
                    |> Task.WhenAll
                    |> Task.WaitAll
                )
                |> ignore
        }
        
        testTask "Default backpressure throws for sendandforget" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MaxPendingMessages(1)
                    .CreateAsync()
                    
            Expect.throwsT2<ProducerQueueIsFullError> (fun () ->
                    [| producer.SendAndForgetAsync([|0uy|]); producer.SendAndForgetAsync([|1uy|]) |]
                    |> Task.WhenAll
                    |> Task.WaitAll
                )
                |> ignore
        }
        
        testTask "Blocking backpressure doesn't throw" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .BlockIfQueueFull(true)
                    .MaxPendingMessages(1)
                    .CreateAsync()
                    
            let! (messageIds : MessageId[]) =
                    [| producer.SendAsync([|0uy|]); producer.SendAsync([|1uy|]); producer.SendAsync([|2uy|]) |]
                    |> Task.WhenAll
                    
            Expect.isLessThan "" (messageIds.[0], messageIds.[1])
            Expect.isLessThan "" (messageIds.[1], messageIds.[2])
        }
        
        testTask "Blocking backpressure doesn't throw for sendAndForget" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .BlockIfQueueFull(true)
                    .MaxPendingMessages(1)
                    .CreateAsync()
                    
            let! _ =
                    [| producer.SendAndForgetAsync([|0uy|]); producer.SendAndForgetAsync([|1uy|]); producer.SendAndForgetAsync([|2uy|]) |]
                    |> Task.WhenAll
                    
            ()
        }
    ]