module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Diagnostics
open Pulsar.Client.Api
open Pulsar.Client.Common

type OTelConsumerInterceptor<'T>() =
    static let  source = "pulsar.consumer"
    let endActivity(consumer:IConsumer<'T>, messageID:MessageId, ``exception``:Exception, ackType, 
                            act : ActivitySource)=              
        let activity = act.StartActivity(consumer.Topic + " consumed",ActivityKind.Consumer)                
        match ``exception`` with
                | null ->
                    activity.SetTag("acknowledge.type", ackType).
                             SetTag("messaging.destination_kind", "topic").
                             SetTag("messaging.destination", consumer.Topic).
                             SetTag("messaging.message_id", messageID).
                             SetTag("messaging.operation", "AfterConsume") |> ignore
                | _ ->
                    activity.SetTag("exception.type", ``exception``.Source).
                             SetTag("exception.message", ``exception``.Message).
                             SetTag("exception.stacktrace", ``exception``.StackTrace) |> ignore
        activity.Stop()          
        ()
      
    member this.activitySource : ActivitySource = new ActivitySource(source)
    static member Source
        with get() = source
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =           
            let activity = this.activitySource.StartActivity(consumer.Topic + " receive",ActivityKind.Consumer)
            if activity <> null then                
                if activity.IsAllDataRequested = true then                   
                   activity.SetTag("messaging.system", "pulsar"). 
                            SetTag("messaging.destination_kind", "topic").
                            SetTag("messaging.destination", consumer.Topic).
                            SetTag("messaging.operation", "BeforeConsume") |> ignore
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