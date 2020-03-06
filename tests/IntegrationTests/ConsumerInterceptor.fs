module Pulsar.Client.IntegrationTests.ConsumerInterceptor

open System
open System.Collections.Generic
open Expecto
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

type ConsumerInterceptorBeforeConsume() =
    member val BeforeMessages = ResizeArray<MessageBuilder>() with get

    interface IConsumerInterceptor with
        member this.Close() = ()
        member this.BeforeConsume(_, message)  =
            let msgValue = message.Data |> Encoding.UTF8.GetString
            let newProp = Dictionary(message.Properties)
            newProp.Add("BeforeConsume", msgValue)
            {message with Properties = newProp}
        
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

    interface IConsumerInterceptor with
        member this.Close() =
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
        testAsync "Check OnClose" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = ConsumerInterceptorOnAcknowledge()
            
            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                        do! consumer.CloseAsync()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            if not consumerInterceptor.Closed then failwith "OnClose missed"
        }
        testAsync "Check BeforeConsume" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(ConsumerInterceptorBeforeConsume())
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
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

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
        }
        testAsync "Check OnAcknowledge" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = ConsumerInterceptorOnAcknowledge()
            
            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.AcknowledgeAsync message.MessageId
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in OnAcknowledge not equal to send messageIds"
        }
        testAsync "Check OnAcknowledgeCumulative" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 9
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = ConsumerInterceptorOnAcknowledge()
            
            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .BatchingMaxMessages(3)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1000.0))
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! fastProduceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                            
                        do! consumer.AcknowledgeCumulativeAsync messageIds.[numberOfMessages - 3]
                        do! consumer.AcknowledgeCumulativeAsync messageIds.[numberOfMessages - 2]
                        do! consumer.AcknowledgeCumulativeAsync messageIds.[numberOfMessages - 1]
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            do! Async.Sleep 200
            let prevMsgAck = consumerInterceptor.AckCumulativeMessageIds.[0]
            let firstAck = consumerInterceptor.AckCumulativeMessageIds.[1]
            let secondAck = consumerInterceptor.AckCumulativeMessageIds.[2]
            let thirdAck = consumerInterceptor.AckCumulativeMessageIds.[3]
            
            match prevMsgAck.Type with
            | Cumulative (indx, batchMsgAcker) ->
                if not (indx = %0 && batchMsgAcker.GetOutstandingAcks() = 0 && batchMsgAcker.GetBatchSize() = 0 && prevMsgAck.EntryId = %1L) then
                    failwith "No interceptor for prevMsgAck"
            | _ -> failwith "Ack type should be Cumulative"

            match firstAck.Type with
            | Cumulative (indx, _) ->
                if not (indx = %0 && firstAck.EntryId = %2L) then
                    failwith "No interceptor for firstAck"
            | _ -> failwith "Ack type should be Cumulative"

            match secondAck.Type with
            | Cumulative (indx, _) ->
                if not (indx = %1 && secondAck.EntryId = %2L) then
                    failwith "No interceptor for secondAck"
            | _ -> failwith "Ack type should be Cumulative"

            match thirdAck.Type with
            | Cumulative (indx, _) ->
                if not (indx = %2 && thirdAck.EntryId = %2L) then
                    failwith "No interceptor for thirdAck"
            | _ -> failwith "Ack type should be Cumulative"
        }
        testAsync "Check OnNegativeAcksSend" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = ConsumerInterceptorOnAcknowledge()
            
            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .NegativeAckRedeliveryDelay(TimeSpan(100L))
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            do! consumer.NegativeAcknowledge message.MessageId
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            do! Async.Sleep 200
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckNegativeMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in NegativeAcknowledge not equal to send messageIds"
        }
        testAsync "Check OnAckTimeoutSend" {

            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let messageIds = ResizeArray<MessageId>()
            let consumerInterceptor = ConsumerInterceptorOnAcknowledge()
            
            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(consumerInterceptor)
                    .AckTimeout(TimeSpan.FromMilliseconds(1001.0))
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        for _ in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            messageIds.Add message.MessageId
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            
            do! Async.Sleep 3000
            
            let inMessagesIdSet = messageIds |> Set.ofSeq
            let ackMessagesIdSet = consumerInterceptor.AckTimeoutMessageIds |> Seq.map id |> Set.ofSeq

            if not (inMessagesIdSet - ackMessagesIdSet).IsEmpty then failwith "MessageIds in AckTimeoutMessageIds not equal to send messageIds"
        }
    ]