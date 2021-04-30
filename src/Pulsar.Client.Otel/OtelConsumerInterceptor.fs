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

type AcknowledgeType =
    | Timeout
    | Ok
    | Cumulative
    | Negative

type OTelConsumerInterceptor<'T>() =
    let Propagator = Propagators.DefaultTextMapPropagator
    static let source = "pulsar.consumer"

    let endActivity(consumer: IConsumer<'T>, messageID: MessageId,exn: Exception, ackType: AcknowledgeType,
                     act: ActivitySource, c: ConcurrentDictionary<MessageId, Activity>) =
        let attachOkTagsAndStop (act: Activity) =
            act.SetTag("acknowledge.type", ackType)
               .SetTag("messaging.destination_kind", "topic")
               .SetTag("messaging.destination", consumer.Topic)
               .SetTag("messaging.message_id", messageID)
               .SetTag("messaging.operation", "AfterConsume")
               |> ignore
            act.Stop()
        
        let stopAllPrevAndCurrent () =
            c
            |> Seq.filter (fun a -> messageID > a.Key)
            |> Seq.iter (fun a -> a.Value |> attachOkTagsAndStop)

        let stopActFromCache() = c.[messageID] |> attachOkTagsAndStop
        let gotActFromCache = c.ContainsKey(messageID)      
        
        let createActivity =
            act.StartActivity(consumer.Topic + " consumed", ActivityKind.Consumer)

        match (exn, ackType, gotActFromCache) with
        | null, Cumulative, true ->
            stopAllPrevAndCurrent()
            stopActFromCache()
        | null, _, true -> stopActFromCache()
        | null, Cumulative, false ->
            createActivity |> attachOkTagsAndStop
            stopAllPrevAndCurrent()
        | null, _, false -> createActivity |> attachOkTagsAndStop
        | _, _, _ ->
            let activity = createActivity
            activity.SetTag("exception.type", exn.Source)
                    .SetTag("exception.message", exn.Message)
                    .SetTag("exception.stacktrace", exn.StackTrace)
                    |> ignore
            activity.Stop()
        ()

    let getter =
        Func<IReadOnlyDictionary<string, string>, string, IEnumerable<string>>
            (fun dict key ->
                match dict.TryGetValue(key) with
                | true, v -> [ v ] :> IEnumerable<string>
                | false, _ -> Enumerable.Empty<string>())

    let cache =   ConcurrentDictionary<MessageId, Activity>()
    let activitySource : ActivitySource = new ActivitySource(source)
    static member Source = source

    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =
            /// Extract the PropagationContext of the upstream parent from the message headers.
            let contextToInject = Unchecked.defaultof<PropagationContext>
            let parentContext = Propagator.Extract(contextToInject, message.Properties, getter)
            Baggage.Current <- parentContext.Baggage //baggage is empty for some reason even I parsed metadata from headers

            let activity =
                activitySource.StartActivity(consumer.Topic + " receive",ActivityKind.Consumer,parentContext.ActivityContext)
            //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
            if activity <> null then
                if activity.IsAllDataRequested = true then
                    cache.TryAdd(message.MessageId, activity) |> ignore                    
            message


        member this.Close() =
            activitySource.Dispose()
            cache.Clear()
            ()

        member this.OnAckTimeoutSend(consumer, messageId) =
            endActivity (consumer, messageId, null, Timeout, activitySource, cache)

        member this.OnAcknowledge(consumer, messageId, ``exception``) =
            endActivity (consumer, messageId, ``exception``, Ok, activitySource, cache)

        member this.OnAcknowledgeCumulative(consumer, messageId, ``exception``) =
            endActivity (consumer, messageId, ``exception``, Cumulative, activitySource, cache)

        member this.OnNegativeAcksSend(consumer, messageId) =
            endActivity (consumer, messageId, null, Negative, activitySource, cache)
