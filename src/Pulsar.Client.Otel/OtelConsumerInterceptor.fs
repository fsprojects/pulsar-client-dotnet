module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open Microsoft.Extensions.Logging
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Api
open Pulsar.Client.Common

type AckResult = Result<string, exn>

type InterceptorCommand =
    | BeforeConsume of MessageId * Activity
    | Timeout of MessageId * AckResult
    | Ack of MessageId * AckResult
    | CumulativeAck of MessageId * AckResult
    | NegativeAck of MessageId * AckResult
    | Stop

type OTelConsumerInterceptor<'T>(sourceName: string, log: ILogger) =
    let cache =  Dictionary<MessageId, Activity>()
    let activitySource = new ActivitySource(sourceName)
    let Propagator = Propagators.DefaultTextMapPropagator
    static let prefix = "OtelConsumerInterceptor:"
    
    let stopActivitySuccessfully (activity: Activity) ackType =
        activity
            .SetTag("acknowledge.type", ackType)
            .Dispose()
    
    let endActivity messageId (ackResult: AckResult) =
        match cache.TryGetValue messageId with
        | true, activity ->
            match ackResult with
            | Ok ackType ->
                 stopActivitySuccessfully activity ackType
            | Error exn ->
                activity
                    .SetTag("acknowledge.type", "Error")
                    .SetTag("exception.type", exn.Source)
                    .SetTag("exception.message", exn.Message)
                    .SetTag("exception.stacktrace", exn.StackTrace)
                    .Dispose()
        | _ ->
            log.LogWarning("{0} Can't find start of activity for msgId={1}", prefix, messageId)
    
    let endPreviousActivities msgId (ackResult: AckResult) =
        cache.Keys
        |> Seq.filter (fun key -> key <= msgId)
        |> Seq.iter (fun key -> endActivity key ackResult)
    
    let getter =
        Func<IReadOnlyDictionary<string, string>, string, IEnumerable<string>>
            (fun dict key ->
                match dict.TryGetValue(key) with
                | true, v -> seq { v }
                | false, _ -> Seq.empty)
    
    let mb =  MailboxProcessor<InterceptorCommand>.Start (fun inbox ->
              
        let rec messageLoop() =
            async {
                let! msg = inbox.Receive() 
                match msg with
                | InterceptorCommand.Ack (msgId, ackResult) ->
                    endActivity msgId ackResult
                    return! messageLoop()
                | InterceptorCommand.NegativeAck (msgId, ackResult) ->
                    endActivity msgId ackResult
                    return! messageLoop()
                | InterceptorCommand.Timeout (msgId, ackResult) ->
                    endActivity msgId ackResult
                    return! messageLoop()
                | InterceptorCommand.CumulativeAck (msgId, ackResult) ->
                    endPreviousActivities msgId ackResult
                    return! messageLoop()
                | InterceptorCommand.BeforeConsume (msgId, activity) ->
                    match cache.TryGetValue msgId with
                    | true, _ ->
                        activity
                            .SetTag("acknowledge.type", "Duplicate")
                            .Dispose()
                    | _ ->
                        cache.Add(msgId, activity)
                    return! messageLoop()
                | InterceptorCommand.Stop ->
                    for KeyValue(_, activity) in cache do 
                        activity
                            .SetTag("acknowledge.type", "InterceptorStopped")
                            .Dispose()
                    activitySource.Dispose()
                    cache.Clear()
                    log.LogInformation("{0} Closed", prefix)
            }       
        messageLoop()
    )
    do mb.Error.Add(fun ex -> log.LogCritical(ex, "{0} mailbox failure", prefix))
    
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =
            // Extract the PropagationContext of the upstream parent from the message headers.
            let contextToInject = Unchecked.defaultof<PropagationContext>
            let parentContext = Propagator.Extract(contextToInject, message.Properties, getter)
            
            // https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
            let activity =
                activitySource.StartActivity(consumer.Topic + " receive",
                                             ActivityKind.Consumer,
                                             parentContext.ActivityContext)            
            if activity |> isNull |> not then
                activity
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", consumer.Topic)
                    .SetTag("messaging.message_id", message.MessageId)
                    .SetTag("messaging.operation", "Consume")
                    |> ignore
                if activity.IsAllDataRequested then
                    parentContext.Baggage.GetBaggage()
                    |> Seq.iter (fun (KeyValue kv) -> 
                        activity.AddBaggage kv |> ignore) 
                    mb.Post <| InterceptorCommand.BeforeConsume(message.MessageId, activity)
            message


        member this.Dispose() =
            mb.Post InterceptorCommand.Stop

        member this.OnAckTimeoutSend(_, messageId) =
            mb.Post <| InterceptorCommand.Timeout(messageId, nameof InterceptorCommand.Timeout |> Ok)

        member this.OnAcknowledge(_, messageId, exn) =
            let ackResult =
                match exn with
                | null ->
                    nameof InterceptorCommand.Ack |> Ok
                | _ ->
                    exn |> Error
            mb.Post <| InterceptorCommand.Ack(messageId, ackResult)

        member this.OnAcknowledgeCumulative(_, messageId, exn) =
            let ackResult =
                match exn with
                | null ->
                    nameof InterceptorCommand.CumulativeAck |> Ok
                | _ ->
                    exn |> Error
            mb.Post <| InterceptorCommand.CumulativeAck(messageId, ackResult)
            
        member this.OnNegativeAcksSend(_, messageId) =
            mb.Post <| InterceptorCommand.NegativeAck(messageId, nameof InterceptorCommand.NegativeAck |> Ok)
            
