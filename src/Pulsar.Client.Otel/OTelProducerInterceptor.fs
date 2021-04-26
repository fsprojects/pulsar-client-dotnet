module Pulsar.Client.Otel.OTelProducerInterceptor

open System.Diagnostics
open OpenTelemetry
open Pulsar.Client.Api
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Common
//https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Api/README.md#instrumenting-a-libraryapplication-with-net-activity-api
type OTelProducerInterceptor<'T>() =
    let Propagator = Propagators.DefaultTextMapPropagator
    
    member this.activitySource : ActivitySource = new ActivitySource("pulsar.producer")
    interface IProducerInterceptor<'T> with        
        member this.BeforeSend(producer, message) =           
            let name = producer.Topic + " send"            
            let activity = this.activitySource.StartActivity(name,ActivityKind.Producer)   
            if activity <> null then     //If there are no listeners interested in this activity, the activity above will be null <..> Ensure that all subsequent calls using this activity are protected with a null check.           
                if activity.IsAllDataRequested = true then // It is highly recommended to check activity.IsAllDataRequested, before populating any tags which are not readily available. IsAllDataRequested is the same as Span.IsRecording and will be false when samplers decide to not record the activity, and this can be used to avoid any expensive operation to retrieve tags.
                   //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
                   activity.SetTag("messaging.system", "pulsar") |> ignore
                   activity.SetTag("messaging.destination_kind", "topic") |> ignore
                   activity.SetTag("messaging.destination", producer.Topic) |> ignore
                   activity.SetTag("messaging.operation", "BeforeSend") |> ignore
                   //add propagator here
                   let contextToInject = activity.Context;
                   Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), message, this.InjectTraceContextIntoBasicProperties);
                    
                   activity.Stop()
            message
        member this.Close() = () //stop activities from before& ack ?
        member this.Eligible _ = true
        member this.OnSendAcknowledgement(producer, _, messageId, ``exception``) =           
            match ``exception`` with
                | null ->
                      let activity = this.activitySource.StartActivity("exception",ActivityKind.Producer)                      
                     //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md
                      if ``exception`` <> null then                        
                          activity.SetTag("exception.type", ``exception``.Source) |> ignore
                          activity.SetTag("exception.message", ``exception``.Message) |> ignore
                          activity.SetTag("exception.stacktrace", ``exception``.StackTrace) |> ignore
                          activity.Stop()
                | _ ->
                     let activity = this.activitySource.StartActivity(producer.Topic + " OnSendAcknowledgement",ActivityKind.Producer)                    
                     if activity <> null then               
                        if activity.IsAllDataRequested = true then
                          activity.SetTag("messaging.destination_kind", "topic") |> ignore
                          activity.SetTag("messaging.destination", producer.Topic) |> ignore
                          activity.SetTag("messaging.operation", "OnSendAcknowledgement") |> ignore
                          activity.SetTag("messaging.message_id", messageId) |> ignore       
                          activity.Stop()  
          
                   