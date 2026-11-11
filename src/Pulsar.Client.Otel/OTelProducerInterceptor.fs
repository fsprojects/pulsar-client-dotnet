module Pulsar.Client.Otel.OTelProducerInterceptor

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open Microsoft.Extensions.Logging
open OpenTelemetry
open Pulsar.Client.Api
open OpenTelemetry.Context.Propagation

//https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Api/README.md#instrumenting-a-libraryapplication-with-net-activity-api
type OTelProducerInterceptor<'T>(sourceName: string, log: ILogger) =
    let Propagator = Propagators.DefaultTextMapPropagator
    static let prefix = "OtelProducerInterceptor:"
    static let activityKey = "traceparent"
    static let traceStateKey = "tracestate"
    let activitySource = new ActivitySource(sourceName)
    let cache = ConcurrentDictionary<string,Activity>()
    // A message can have these properties already, as one has whose properties are copied from a consumed message.
    // They are replaced, which is what a propagator expects of its setter.
    let setter =
        Action<Dictionary<string,string>,string,string>
            (fun msg key value -> msg.[key] <- value)

    // Ending an activity makes the one that was current where it started the current one of whoever ends it. For
    // the activity of a send that is the producer's loop, or whoever disposes the interceptor, which would go on in
    // the activity of the application that sent the message. So what is current is left as it is, unless it is the
    // activity that ends.
    let endActivity (activity: Activity) =
        let current = Activity.Current
        try
            activity.Dispose()
        finally
            if not (obj.ReferenceEquals(current, activity)) then
                // one that has ended can't be made current again
                Activity.Current <- if isNull current || current.IsStopped then null else current

    let addToCache activityId activity =
        match cache.TryAdd(activityId, activity) with
        | true ->
            ()
        | _ ->
            activity.SetTag("messaging.acknowledge_type", "Duplicate")
            |> endActivity
            log.LogWarning("{0} Duplicate activity detected", prefix)

    interface IProducerInterceptor<'T> with
        member this.BeforeSend(producer, message) =
            let mutableDict =  Dictionary<string,string>(message.Properties.Count)
            message.Properties
            |> Seq.iter (fun (KeyValue kv) -> mutableDict.Add kv)


            let activity =
                activitySource.StartActivity(producer.Topic + " send", ActivityKind.Producer)

            if isNull activity then
                message  //If there are no listeners interested in this activity, the activity above will be null.
            else
                activity
                    .SetTag("messaging.system", "pulsar")
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", producer.Topic)
                    .SetTag("messaging.operation", "send")
                    .SetTag("messaging.producer_id",$"{producer.Name} - {producer.ProducerId}")
                    |> ignore
                if activity.IsAllDataRequested then
                   // It is highly recommended to check activity.IsAllDataRequested,
                   // before populating any tags which are not readily available.
                   // IsAllDataRequested is the same as Span.IsRecording and will be false
                   // when samplers decide to not record the activity,
                   // and this can be used to avoid any expensive operation to retrieve tags.
                   //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
                   //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageSender.cs#L102
                   let contextToInject = activity.Context
                   // A message can have a trace already, as one has whose properties are copied from a consumed
                   // message. It is removed before the propagator sets that of this send: the propagator sets a
                   // trace state only if this send has one, so that of the other trace would go out with this
                   // one, and a propagator that sets nothing would leave the other trace to be taken for this one.
                   mutableDict.Remove activityKey |> ignore
                   mutableDict.Remove traceStateKey |> ignore
                   Propagator.Inject(PropagationContext(contextToInject, Baggage.Current), mutableDict, setter)
                   if not (mutableDict.ContainsKey activityKey) then
                       log.LogWarning("activityKey {0} is missing in dictionary. Check if OTEL propagators are configured correctly.", activityKey)
                       message // OTEL listeners are not configured correctly, so return message as is
                   else
                       addToCache mutableDict.[activityKey] activity
                       message.WithProperties(mutableDict)
                else
                   //don't handle activity
                   message

        member this.Dispose() =
            // Ending an activity runs the code of its listeners, and what one of them throws must not leave the rest
            // open, nor the activity source undisposed.
            try
                for KeyValue(_, activity) in cache do
                    try
                        activity.SetTag("messaging.acknowledge_type", "InterceptorStopped")
                        |> endActivity
                    with ex ->
                        log.LogError(ex, "{0} failed to end an open activity", prefix)
            finally
                cache.Clear()
                activitySource.Dispose()
            log.LogInformation("{0} Closed", prefix)

        member this.Eligible _ = true

        member this.OnSendAcknowledgement(_, builder, messageId, exn) =
            match builder.Properties.TryGetValue(activityKey) with
            | true, activityId ->
                match cache.TryRemove(activityId) with
                | true, activity ->
                    match exn with
                    | null ->
                         activity
                            .SetTag("messaging.acknowledge_type", "Success")
                            .SetTag("messaging.message_id", messageId)
                         |> endActivity
                    |  _ ->
                        //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md
                        activity
                            .SetTag("messaging.acknowledge_type", "Error")
                            .SetTag("exception.type", exn.GetType().FullName)
                            .SetTag("exception.message", exn.Message)
                            .SetTag("exception.stacktrace", exn.StackTrace)
                        |> endActivity
                | _ ->
                    log.LogWarning("{0} Can't find start of activity for msgId={1}", prefix, messageId)
            | _ ->
                log.LogDebug("{0} activity id is missing for msgId={1}", prefix, messageId)
