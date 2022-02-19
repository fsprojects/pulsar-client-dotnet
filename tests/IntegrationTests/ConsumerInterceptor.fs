﻿module Pulsar.Client.IntegrationTests.ConsumerInterceptor

open System
open System.Collections.Generic
open Expecto
open Pulsar.Client.Api

open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

type ConsumerInterceptorBeforeConsume() =
    member val BeforeMessages = ResizeArray<MessageBuilder<byte[]>>() with get

    interface IConsumerInterceptor<byte[]> with
        member this.Dispose() = ()
        member this.BeforeConsume(_, message)  =
            let msgValue = message.Data |> Encoding.UTF8.GetString
            let newProp = Dictionary(message.Properties)
            newProp.Add("BeforeConsume", msgValue)
            message.WithProperties(newProp)
        
        member this.OnAcknowledge(_,_,_) = ()
        member this.OnAcknowledgeCumulative(_,_,_) = ()
        member this.OnNegativeAcksSend(_,_) = ()
        member this.OnAckTimeoutSend(_,_) = ()

type ConsumerInterceptorOnAcknowledge() =
    member val Closed = false with get, set
    member val AckMessageIds = ResizeArray<MessageId>() with get
    member val AckCumulativeMessageIds = ResizeArray<MessageId>() with get
    member val AckNegativeMessageIds = ResizeArray<MessageId>() with get
    member val AckTimeoutMessageIds = ResizeArray<MessageId>() with get

    interface IConsumerInterceptor<byte[]> with
        member this.Dispose() =
            this.Closed <- true
        member this.BeforeConsume(_, message) = message
        member this.OnAcknowledge(_, messageId, _) =
            this.AckMessageIds.Add messageId 
        member this.OnAcknowledgeCumulative(_, messageId, _) =
            this.AckCumulativeMessageIds.Add messageId
        member this.OnNegativeAcksSend(_, messageId) =
            this.AckNegativeMessageIds.Add messageId
        member this.OnAckTimeoutSend(_, messageId) =
            this.AckTimeoutMessageIds.Add messageId


[<Tests>]
let tests =
    testList "ConsumerInterceptor" [
        
        testTask "Check OnClose" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = new ConsumerInterceptorOnAcknowledge()
            let interceptName = "OnClose"
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                        do! consumer.DisposeAsync()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 

            if not consumerInterceptor.Closed then failwith "OnClose missed"
        }
        
        testTask "Check BeforeConsume" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let interceptName = "BeforeConsume"

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(new ConsumerInterceptorBeforeConsume())
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            let messageValue = message.Data |> Encoding.UTF8.GetString
                            let beforeValue = message.Properties.Item "BeforeConsume"
                            if messageValue <> beforeValue then failwith "The BeforeConsume properties is not equal to message data"
                            
                        ()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
        }
        
        testTask "Check OnAcknowledge" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = new ConsumerInterceptorOnAcknowledge()
            let interceptName = "OnAcknowledge"
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
            do! Task.Delay(110) // wait for acks

            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in OnAcknowledge not equal to send messageIds"
        }
        
        testTask "Check OnAcknowledgeCumulative" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 9
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = new ConsumerInterceptorOnAcknowledge()
            let interceptName = "OnAcknowledgeCumulative"
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .BatchingMaxMessages(3)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1000.0))
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                            
                        do! consumer.AcknowledgeCumulativeAsync messageIds.[numberOfMessages - 2]
                        do! consumer.AcknowledgeCumulativeAsync messageIds.[numberOfMessages - 1]
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask)
            
            do! Task.Delay 200
            let prevMsgId = consumerInterceptor.AckCumulativeMessageIds.[0]
            let firstId = consumerInterceptor.AckCumulativeMessageIds.[1]
            let secondId = consumerInterceptor.AckCumulativeMessageIds.[2]
            
            match prevMsgId.Type with
            | Batch _ -> failwith "MessageIdType should be Individual"
            | Single ->
                if not (prevMsgId.EntryId = %1L) then
                    failwith "No interceptor for prevMsgAck"

            match firstId.Type with
            | Batch (indx, _) ->
                if not (indx = %1 && firstId.EntryId = %2L) then
                    failwith "No interceptor for firstAck"
            | _ -> failwith "MessageIdType should be Cumulative"

            match secondId.Type with
            | Batch (indx, _) ->
                if not (indx = %2 && secondId.EntryId = %2L) then
                    failwith "No interceptor for secondAck"
            | _ -> failwith "MessageIdType should be Cumulative"
        }
        
        testTask "Check OnNegativeAcksSend" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = new ConsumerInterceptorOnAcknowledge()
            let interceptName = "OnNegativeAcksSend"
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .NegativeAckRedeliveryDelay(TimeSpan(100L))
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.NegativeAcknowledge message.MessageId
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
            
            do! Task.Delay 200
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckNegativeMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in NegativeAcknowledge not equal to send messageIds"
        }
        
        testTask "Check OnAckTimeoutSend" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = new ConsumerInterceptorOnAcknowledge()
            let interceptName = "OnAckTimeoutSend"
            
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(interceptName)
                    .CreateAsync() 
            
            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(interceptName)
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .AckTimeout(TimeSpan.FromMilliseconds(1000.0))
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages interceptName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
            
            do! Task.Delay 6000
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckTimeoutMessageIds |> Seq.map id |> Set.ofSeq
            
            do! consumer.UnsubscribeAsync() 
            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then
                let diff = inMessagesIdSet - ackMessagesIdSet |> Set.map(fun m -> string m.EntryId) |> String.concat ";"
                failwith $"MessageIds in AckTimeoutMessageIds not equal to send messageIds: {diff}"
        }
    ]
