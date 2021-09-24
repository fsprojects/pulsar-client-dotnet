module Pulsar.Client.IntegrationTests.ProducerInterceptor

open System
open System.Collections.Generic
open Expecto
open Pulsar.Client.Api

open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common

type ProducerInterceptorEligible() =
    member val BeforeMessages = ResizeArray<MessageBuilder<byte[]>>() with get
    interface IProducerInterceptor<byte[]> with
        member this.Dispose() = ()
        
        member this.Eligible(message) =
            match message.Properties.GetValueOrDefault("Eligible") with
            | "false" -> false
            | _ ->  true
        
        member this.BeforeSend(_, message) =
            this.BeforeMessages.Add message
            message
        
        member this.OnSendAcknowledgement(_, _, _, _) = ()

type ProducerInterceptorBefore() =
    interface IProducerInterceptor<byte[]> with
        member this.Dispose() = ()
        
        member this.Eligible _ = true
        
        member this.BeforeSend(_, message) =
            let msgValue = message.Value |> Encoding.UTF8.GetString    
            let newProp = Dictionary(message.Properties)
            newProp.Add("BeforeSend", msgValue)
            message.WithProperties(newProp)
        
        member this.OnSendAcknowledgement(_, _, _, _) = ()

type ProducerInterceptorSendAck() =
    member val Closed = false with get, set
    member val AckMessages = ResizeArray<MessageBuilder<byte[]>>() with get
    member val AckMessageIds = ResizeArray<MessageId>() with get

    interface IProducerInterceptor<byte[]> with
        member this.Dispose() =
            this.Closed <- true
        
        member this.Eligible _ = true
        
        member this.BeforeSend(_, message) = message
        
        member this.OnSendAcknowledgement(_, message, messageId, _) =
            this.AckMessages.Add message
            this.AckMessageIds.Add messageId
            ()
    
[<Tests>]
let tests =
    testList "ProducerInterceptor" [
        testAsync "Check OnClose" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let prodInterceptor = new ProducerInterceptorSendAck()
            let interceptName = "OnClose"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .Intercept(prodInterceptor)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let messages =
                generateMessages numberOfMessages interceptName
                |> Seq.map Encoding.UTF8.GetBytes

            let producerTask =
                Task.Run(fun () ->
                    task {
                        for msg in messages do
                            let! msgId = producer.SendAsync(msg)
                            messageIds.Add msgId
                        do! producer.DisposeAsync()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                    }:> Task)
            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            
            if not prodInterceptor.Closed then failwith "OnClose missed"
        }
        testAsync "Check Eligible" {
            let client = getClient()
            let numberOfMessages = 10
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let prodInterceptor = new ProducerInterceptorEligible()
            let interceptName = "Eligible"

            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .Intercept(prodInterceptor)
                    .CreateAsync() |> Async.AwaitTask
          

            let eligibleMessages =
                let property = dict ["Eligible", "true"] |> Dictionary
                generateMessages numberOfMessages interceptName
                |> Seq.map(fun msg -> producer.NewMessage(Encoding.UTF8.GetBytes(msg), properties = property))

            let noEligibleMessages =
                let property = dict ["Eligible", "false"] |> Dictionary
                generateMessages numberOfMessages "No_Eligible"
                |> Seq.map(fun msg -> producer.NewMessage(Encoding.UTF8.GetBytes(msg), properties = property))
                
            let allMessages =
                seq {
                    yield! eligibleMessages
                    yield! noEligibleMessages
                }
            let producerTask =
                Task.Run(fun () ->
                    task {
                        for msg in allMessages do
                            let! _ = producer.SendAsync(msg)
                            ()
                    } :> Task )

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            ()
                    }:> Task)
            
            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            let inEligibleMessages = eligibleMessages |> Seq.map(fun msg -> msg.Value |> Encoding.UTF8.GetString) |> Set.ofSeq
            let beforeMessagesSet = prodInterceptor.BeforeMessages |> Seq.map(fun msg -> msg.Value |> Encoding.UTF8.GetString) |> Set.ofSeq
            if not (beforeMessagesSet - inEligibleMessages).IsEmpty then failwith "BeforeMessagesSet not equal inEligibleMessages"
            ()
        }
        testAsync "Check BeforeSend" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let interceptName = "BeforeSend"
            
            let prodInterceptor = new ProducerInterceptorBefore()
            let! producer =
                client.NewProducer()
                    .ProducerName(interceptName)
                    .EnableBatching(false)
                    .Topic(topicName)
                    .Intercept(prodInterceptor)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let messages = generateMessages numberOfMessages interceptName
                        for msg in messages do
                            let! _ = producer.SendAsync(Encoding.UTF8.GetBytes(msg))
                            ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            let messageValue = message.Data |> Encoding.UTF8.GetString
                            let beforeValue = message.Properties.Item "BeforeSend"
                            if messageValue <> beforeValue then failwith "The BeforeSend properties is not equal to message data"
                        ()
                    }:> Task)
            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
        }
        testAsync "Check OnSendAcknowledgement" {
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let prodInterceptor = new ProducerInterceptorSendAck()
            let interceptName = "OnSendAcknowledgement"
            
            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .Intercept(prodInterceptor)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let messages =
                generateMessages numberOfMessages interceptName
                |> Seq.map Encoding.UTF8.GetBytes
                |> Seq.map (fun bytes -> producer.NewMessage(bytes))

            let producerTask =
                Task.Run(fun () ->
                    task {
                        for msg in messages do
                            let! msgId = producer.SendAsync(msg)
                            messageIds.Add msgId
                            ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            ()
                    }:> Task)
            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            let inMessages = messages |> Seq.map(fun msg -> msg.Value |> Encoding.UTF8.GetString) |> Set.ofSeq
            let ackMessagesSet = prodInterceptor.AckMessages |> Seq.map(fun msg -> msg.Value |> Encoding.UTF8.GetString) |> Set.ofSeq
            
            if not (inMessages - ackMessagesSet).IsEmpty then failwith "Messages in OnSendAcknowledgement not equal to send messages"
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = prodInterceptor.AckMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in OnSendAcknowledgement not equal to send messageIds"
        }
    ]
