module Pulsar.Client.IntegrationTests.Interceptors

open System
open System.Threading
open System.Diagnostics

open System
open System.Collections.Generic
open System.Linq.Expressions
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX

type ProducerInterceptor() =
    let mutable ackOk = true
    member this.AckOk with get() = ackOk 
    interface IProducerInterceptor with
        member this.Close() = ()
        
        member this.Eligible(message:MessageBuilder) = true
        
        member this.BeforeSend(producer:IProducer, message:MessageBuilder) =
            let newProp = Dictionary(message.Properties)
            newProp.Add("BeforeSend", "1")
            MessageBuilder(message.Value, %message.Key, newProp, message.DeliverAt)
        
        member this.OnSendAcknowledgement(producer:IProducer, message:MessageBuilder, messageId:MessageId, exn:Exception) =
            let v = message.Properties.Item "BeforeSend"
            if v <> "1" then ackOk <- false
            ()
    
type ConsumerInterceptor() =
    
    interface IConsumerInterceptor with
        member this.Close() = ()
        member this.BeforeConsume(_, message)  =
            let newProp = Dictionary(message.Properties)
            newProp.Add("BeforeConsume", "1")
            {message with Properties = newProp}
        
        member this.OnAcknowledge(_,_,_) = ()
        member this.OnAcknowledgeCumulative(_,_,_) = ()
        member this.OnNegativeAcksSend(_,_,_) = ()
        member this.OnAckTimeoutSend(_,_,_) = ()

[<Tests>]
let tests =
    testList "Interceptor" [
        testList "ProducerInterceptor" [
            testAsync "Send and receive 10 messages and check BeforeSend" {
                let client = getClient()
                let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
                let numberOfMessages = 10
                
                let prodInterceptor = ProducerInterceptor()
                let! producer =
                    ProducerBuilder(client)
                        .Topic(topicName)
                        .Intercept(prodInterceptor)
                        .CreateAsync() |> Async.AwaitTask
                
                let! consumer =
                    ConsumerBuilder(client)
                        .Topic(topicName)
                        .ConsumerName("concurrent")
                        .SubscriptionName("test-subscription")
                        .SubscribeAsync() |> Async.AwaitTask

                let producerTask =
                    Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages "concurrent"
                        }:> Task)

                let consumerTask =
                    Task.Run(fun () ->
                        task {
                            let mutable acc: int = 0 
                            for i in 1..numberOfMessages do
                                let! message = consumer.ReceiveAsync()
                                let value = message.Properties.Item "BeforeSend"
                                if value = "1" then acc <- acc + 1
                            if acc <> numberOfMessages then failwith "The number of BeforeSend properties is not equal to numberOfMessages"
                            ()
                        }:> Task)

                do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
                if not prodInterceptor.AckOk then failwith "The BeforeSend properties is not equal to 1"
                Log.Debug("Finished Send and receive 10 messages and check BeforeConsume")
            }
        ]
        testList "ConsumerInterceptor" [
            testAsync "Send and receive 10 messages and check BeforeConsume" {

                Log.Debug("Started Send and receive 10 messages and check BeforeConsume")
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
                        .Intercept(ConsumerInterceptor())
                        .SubscribeAsync() |> Async.AwaitTask

                let producerTask =
                    Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages "concurrent"
                        }:> Task)

                let consumerTask =
                    Task.Run(fun () ->
                        task {
                            let mutable acc: int = 0 
                            for i in 1..numberOfMessages do
                                let! message = consumer.ReceiveAsync()
                                let value = message.Properties.Item "BeforeConsume"
                                if value = "1" then acc <- acc + 1
                            if acc <> numberOfMessages then failwith "The number of BeforeConsume properties is not equal to numberOfMessages"
                            ()
                        }:> Task)

                do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
                Log.Debug("Finished Send and receive 10 messages and check BeforeConsume")
            }
        ]
    ]