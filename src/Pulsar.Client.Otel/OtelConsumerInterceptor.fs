module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Concurrent
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
                            act : ActivitySource, c:ConcurrentDictionary<MessageId,Activity>)=              
       
        let gotActFromCache = c.ContainsKey(messageID)
        let attachOkTagsAndStop (act:Activity) =
             act.SetTag("acknowledge.type", ackType).
                                 SetTag("messaging.destination_kind", "topic").
                                 SetTag("messaging.destination", consumer.Topic).
                                 SetTag("messaging.message_id", messageID).
                                 SetTag("messaging.operation", "AfterConsume") |> ignore
             act.Stop()
        
        if gotActFromCache then
                c.[messageID] |> attachOkTagsAndStop   
        else
            let activity = act.StartActivity(consumer.Topic + " consumed",ActivityKind.Consumer)
            match ``exception`` with
                    | null ->
                        activity |> attachOkTagsAndStop   
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
    let cache = ConcurrentDictionary<MessageId,Activity>()
    let activitySource : ActivitySource = new ActivitySource(source)
    
    static member Source
        with get() = source
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =
            /// Extract the PropagationContext of the upstream parent from the message headers.
            let mutableDict = message.Properties
            let contextToInject = Unchecked.defaultof<PropagationContext>   //https://stackoverflow.com/questions/2246206/what-is-the-equivalent-in-f-of-the-c-sharp-default-keyword
            let parentContext = Propagator.Extract(contextToInject,mutableDict,getter)
            Baggage.Current <- parentContext.Baggage //baggage is empty for some reason even I parsed metadata from headers
            let activity = activitySource.StartActivity(consumer.Topic + " receive",ActivityKind.Consumer, parentContext.ActivityContext)
            //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
            if activity <> null then                
                if activity.IsAllDataRequested = true then                   
                   activity.SetTag("messaging.system", "pulsar"). 
                            SetTag("messaging.destination_kind", "topic").
                            SetTag("messaging.destination", consumer.Topic).
                            SetTag("messaging.operation", "BeforeConsume") |> ignore                  
                   cache.TryAdd(message.MessageId,activity) |> ignore                   
                   ()
            message
           
           
        member this.Close() =
            activitySource.Dispose()
            ()
        member this.OnAckTimeoutSend(consumer, messageId) =
            endActivity(consumer,messageId, null, "AcknowledgeType.Timeout",activitySource,cache)
        member this.OnAcknowledge(consumer, messageId, ``exception``) =
            endActivity(consumer,messageId, ``exception``, "AcknowledgeType.Ok", activitySource,cache)
        member this.OnAcknowledgeCumulative(consumer, messageId, ``exception``) =
            endActivity(consumer,messageId, ``exception``, "AcknowledgeType.Cumulative", activitySource,cache)
        member this.OnNegativeAcksSend(consumer, messageId) =
            endActivity(consumer,messageId, null, "AcknowledgeType.Negative", activitySource,cache)