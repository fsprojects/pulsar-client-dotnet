module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open System.Linq
open Microsoft.Extensions.Logging
open OpenTelemetry
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

type OTelConsumerInterceptor<'T>(log: ILogger) =
    let cache =  Dictionary<MessageId, Activity>()
    static let source = "pulsar.consumer"
    let activitySource = new ActivitySource(source)
    let Propagator = Propagators.DefaultTextMapPropagator

    
    let stopActivitySuccessfully (activity: Activity) ackType =
        activity
            .SetTag("acknowledge.type", ackType)
            .Stop()
    
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
                    .Stop()
        | _ ->
            log.LogWarning("Can't find start of activity for msgId={0}", messageId)
    
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
                | InterceptorCommand.Stop ->
                    cache
                    |> Seq.iter (fun (KeyValue(_, value)) ->
                        value.SetTag("acknowledge.type", "InterceptorStopped")|> ignore
                        value.Stop())
                    activitySource.Dispose()
                    cache.Clear()
            }       
        messageLoop()
    )
    do mb.Error.Add(fun ex -> log.LogCritical(ex, "{0} otel consumer mailbox failure"))
    
    static member Source = source
    
    interface IConsumerInterceptor<'T> with
        member this.BeforeConsume(consumer, message) =
            // Extract the PropagationContext of the upstream parent from the message headers.
            let contextToInject = Unchecked.defaultof<PropagationContext>
            let parentContext = Propagator.Extract(contextToInject, message.Properties, getter)
            Baggage.Current <- parentContext.Baggage //baggage is empty for some reason even I parsed metadata from headers

            // https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
            let activity =
                activitySource.StartActivity(consumer.Topic + " receive",
                                             ActivityKind.Consumer,
                                             parentContext.ActivityContext)
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", consumer.Topic)
                    .SetTag("messaging.message_id", message.MessageId)
                    .SetTag("messaging.operation", "Consume")
            
            if activity <> null then
                if activity.IsAllDataRequested = true then
                    cache.Add(message.MessageId, activity)                   
            message


        member this.Close() =
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
            
