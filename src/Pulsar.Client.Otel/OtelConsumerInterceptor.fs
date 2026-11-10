module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading.Channels
open Microsoft.Extensions.Logging
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Threading.Tasks

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
            .SetTag("messaging.acknowledge_type", ackType)
            .Dispose()

    let endActivity messageId (ackResult: AckResult) =
        match cache.TryGetValue messageId with
        | true, activity ->
            // Ending an activity runs the code of its listeners. Whatever one of them throws, the activity has ended,
            // and would otherwise stay here for as long as the consumer runs.
            try
                match ackResult with
                | Ok ackType ->
                     stopActivitySuccessfully activity ackType
                | Error exn ->
                    activity
                        .SetTag("messaging.acknowledge_type", "Error")
                        .SetTag("exception.type", exn.GetType().FullName)
                        .SetTag("exception.message", exn.Message)
                        .SetTag("exception.stacktrace", exn.StackTrace)
                        .Dispose()
            finally
                cache.Remove messageId |> ignore
        | _ ->
            log.LogDebug("{0} Can't find start of activity for msgId={1}", prefix, messageId)

    let endPreviousActivities msgId (ackResult: AckResult) =
        cache.Keys
        |> Seq.filter (fun key -> key <= msgId)
        |> Seq.iter (fun key ->
            // each on its own, so that what a listener throws for one doesn't leave the others open
            try
                endActivity key ackResult
            with ex ->
                log.LogError(ex, "{0} failed to end the activity of msgId={1}", prefix, key))

    let getter =
        Func<IReadOnlyDictionary<string, string>, string, IEnumerable<string>>
            (fun dict key ->
                match dict.TryGetValue(key) with
                | true, v -> seq { v }
                | false, _ -> Seq.empty)

    // Nothing ends an activity once the loop has stopped, so it ends the ones that are open, and no more are started.
    // Ending an activity runs the code of its listeners, and what one of them throws must not leave the rest open.
    let endOpenActivities () =
        try
            for KeyValue(_, activity) in cache do
                try
                    activity
                        .SetTag("messaging.acknowledge_type", "InterceptorStopped")
                        .Dispose()
                with ex ->
                    log.LogError(ex, "{0} failed to end an open activity", prefix)
        finally
            cache.Clear()
            activitySource.Dispose()

    let mb = Channel.CreateUnbounded<InterceptorCommand>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            try
                match msg with
                | InterceptorCommand.Ack (msgId, ackResult) ->
                    endActivity msgId ackResult
                | InterceptorCommand.NegativeAck (msgId, ackResult) ->
                    endActivity msgId ackResult
                | InterceptorCommand.Timeout (msgId, ackResult) ->
                    endActivity msgId ackResult
                | InterceptorCommand.CumulativeAck (msgId, ackResult) ->
                    endPreviousActivities msgId ackResult
                | InterceptorCommand.BeforeConsume (msgId, activity) ->
                    match cache.TryGetValue msgId with
                    | true, _ ->
                        activity
                            .SetTag("messaging.acknowledge_type", "Duplicate")
                            .Dispose()
                    | _ ->
                        cache.Add(msgId, activity)
                | InterceptorCommand.Stop ->
                    endOpenActivities()
                    log.LogInformation("{0} Closed", prefix)
                    continueLoop <- false
            // what fails for one message must not end the tracing of all the later ones
            with ex ->
                log.LogError(ex, "{0} failed to process a command", prefix)
                match msg with
                | InterceptorCommand.Stop -> continueLoop <- false
                | _ -> ()
        } :> Task).ContinueWith(fun t ->
            // Nothing reads the mailbox any more, so it takes no more commands: they would pile up with their
            // activities for as long as the consumer runs.
            mb.Writer.TryComplete() |> ignore
            let mutable msg = Unchecked.defaultof<InterceptorCommand>
            while mb.Reader.TryRead(&msg) do
                match msg with
                | InterceptorCommand.BeforeConsume (_, activity) ->
                    try
                        activity.Dispose()
                    with _ ->
                        ()
                | _ ->
                    ()
            if t.IsFaulted || t.IsCanceled then
                // what a Stop would have done
                try
                    endOpenActivities()
                with ex ->
                    log.LogError(ex, "{0} failed to end the open activities", prefix)
                // a cancelled task has no exception of its own: what cancelled it is thrown to whoever waits for it
                let failure: exn =
                    if t.IsFaulted then
                        t.Exception :> exn
                    else
                        try
                            t.GetAwaiter().GetResult()
                            null
                        with ex ->
                            ex
                log.LogCritical(failure, "{0} mailbox failure", prefix)
            else
                log.LogInformation("{0} mailbox has stopped normally", prefix))
        |> ignore

    let postMb msg = mb.Writer.TryWrite(msg) |> ignore

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
                    .SetTag("messaging.system", "pulsar")
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", consumer.Topic)
                    .SetTag("messaging.consumer_id", $"{consumer.Name} - {consumer.ConsumerId}")
                    .SetTag("messaging.message_id", message.MessageId)
                    .SetTag("messaging.operation", "receive")
                    |> ignore
                if activity.IsAllDataRequested then
                    parentContext.Baggage.GetBaggage()
                    |> Seq.iter (fun (KeyValue kv) ->
                        activity.AddBaggage kv |> ignore)
                    if not (mb.Writer.TryWrite(InterceptorCommand.BeforeConsume(message.MessageId, activity))) then
                        // the loop has stopped, so nothing else would end this activity
                        activity.Dispose()
                else
                   //don't handle activity
                    ()
            message


        member this.Dispose() =
            postMb InterceptorCommand.Stop

        member this.OnAckTimeoutSend(_, messageId) =
            postMb (InterceptorCommand.Timeout(messageId, nameof InterceptorCommand.Timeout |> Ok))

        member this.OnAcknowledge(_, messageId, exn) =
            let ackResult =
                match exn with
                | null ->
                    nameof InterceptorCommand.Ack |> Ok
                | _ ->
                    exn |> Error
            postMb (InterceptorCommand.Ack(messageId, ackResult))

        member this.OnAcknowledgeCumulative(_, messageId, exn) =
            let ackResult =
                match exn with
                | null ->
                    nameof InterceptorCommand.CumulativeAck |> Ok
                | _ ->
                    exn |> Error
            postMb (InterceptorCommand.CumulativeAck(messageId, ackResult))

        member this.OnNegativeAcksSend(_, messageId) =
            postMb (InterceptorCommand.NegativeAck(messageId, nameof InterceptorCommand.NegativeAck |> Ok))

