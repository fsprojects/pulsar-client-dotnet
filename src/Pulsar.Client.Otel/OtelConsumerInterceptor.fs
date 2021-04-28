module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open System.Linq
open OpenTelemetry
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Api
open Pulsar.Client.Common

type OTelConsumerInterceptor<'T>() =

    let Propagator = Propagators.DefaultTextMapPropagator
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
    let getter = Func<IReadOnlyDictionary<string,string>,string,IEnumerable<string>>(fun dict key ->
                        match dict.TryGetValue(key) with
                        | true, v -> [v] :> IEnumerable<string>
                        | false, _ -> Enumerable.Empty<string>()
                        )  
    member this.activitySource : ActivitySource = new ActivitySource(source)
    static member Source
        with get() = source
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =
            /// Extract the PropagationContext of the upstream parent from the message headers.
            let mutableDict = message.Properties
            let contextToInject = Unchecked.defaultof<PropagationContext>   //https://stackoverflow.com/questions/2246206/what-is-the-equivalent-in-f-of-the-c-sharp-default-keyword
            let parentContext = Propagator.Extract(contextToInject,mutableDict,getter)
            Baggage.Current <- parentContext.Baggage //baggage is empty for some reason even I parsed metadata from headers
            let activity = this.activitySource.StartActivity(consumer.Topic + " receive",ActivityKind.Consumer, parentContext.ActivityContext)
            //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
            if activity <> null then                
                if activity.IsAllDataRequested = true then                   
                   activity.SetTag("messaging.system", "pulsar"). 
                            SetTag("messaging.destination_kind", "topic").
                            SetTag("messaging.destination", consumer.Topic).
                            SetTag("messaging.operation", "BeforeConsume") |> ignore
                   activity.Stop()
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