module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Concurrent
open System.Diagnostics
open Pulsar.Client.Api
open Pulsar.Client.Common

type OTelConsumerInterceptor<'T>() =
    let endActivity(consumer:IConsumer<'T>, messageID:MessageId, ``exception``:Exception, ackType, 
                            act : ActivitySource)=              
        let activity = act.StartActivity(consumer.Topic + " consumed",ActivityKind.Consumer)                
        match ``exception`` with
                | null ->
                    activity.SetTag("acknowledge.type", ackType) |> ignore
                    activity.SetTag("messaging.destination_kind", "topic") |> ignore
                    activity.SetTag("messaging.destination", consumer.Topic) |> ignore
                    activity.SetTag("messaging.message_id", messageID) |> ignore
                    activity.SetTag("messaging.operation", "AfterConsume") |> ignore
                | _ ->
                    activity.SetTag("exception.type", ``exception``.Source) |> ignore
                    activity.SetTag("exception.message", ``exception``.Message) |> ignore
                    activity.SetTag("exception.stacktrace", ``exception``.StackTrace) |> ignore
        activity.Stop()          
        ()
      
    member this.activitySource : ActivitySource = new ActivitySource("pulsar.consumer")
    
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =           
            let activity = this.activitySource.StartActivity(consumer.Topic + " receive",ActivityKind.Consumer)
            if activity <> null then                
                if activity.IsAllDataRequested = true then                   
                   activity.SetTag("messaging.system", "pulsar") |> ignore
                   activity.SetTag("messaging.destination_kind", "topic") |> ignore
                   activity.SetTag("messaging.destination", consumer.Topic) |> ignore
                   activity.SetTag("messaging.operation", "BeforeConsume") |> ignore
                   activity.Stop()
                  //extract propagator here
            message
           
           
        member this.Close() = ()
        member this.OnAckTimeoutSend(consumer, messageId) =
            endActivity(consumer,messageId, null, "AcknowledgeType.Timeout",this.activitySource)
        member this.OnAcknowledge(consumer, messageId, ``exception``) =
            endActivity(consumer,messageId, ``exception``, "AcknowledgeType.Ok", this.activitySource)
        member this.OnAcknowledgeCumulative(consumer, messageId, ``exception``) =
            endActivity(consumer,messageId, ``exception``, "AcknowledgeType.Cumulative", this.activitySource)
        member this.OnNegativeAcksSend(consumer, messageId) =
            endActivity(consumer,messageId, null, "AcknowledgeType.Negative", this.activitySource)