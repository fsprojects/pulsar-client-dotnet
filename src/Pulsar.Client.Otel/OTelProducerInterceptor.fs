module Pulsar.Client.Otel.OTelProducerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open OpenTelemetry
open Pulsar.Client.Api
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Common

//https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Api/README.md#instrumenting-a-libraryapplication-with-net-activity-api
type OTelProducerInterceptor<'T>() =
    let Propagator = Propagators.DefaultTextMapPropagator
   
    let test = Action<MessageBuilder<'T>,string,string>(fun msg key value ->
         let q= new Dictionary<string,string>()
         let enum = msg.Properties.GetEnumerator()
         while (enum.MoveNext()) do q.Add (enum.Current.Key,enum.Current.Value)
         q.Add(key,value)
         let newMsg = msg.WithProperties q //no way to return new message from Action
         ()
           
        )
    static let  source = "pulsar.producer"
    member this.activitySource : ActivitySource = new ActivitySource(source)
    static member Source
        with get() = source
    interface IProducerInterceptor<'T> with        
        member this.BeforeSend(producer, message) =           
            let name = producer.Topic + " send"            
            let activity = this.activitySource.StartActivity(name,ActivityKind.Producer)   
            if activity <> null then     //If there are no listeners interested in this activity, the activity above will be null <..> Ensure that all subsequent calls using this activity are protected with a null check.           
                if activity.IsAllDataRequested = true then // It is highly recommended to check activity.IsAllDataRequested, before populating any tags which are not readily available. IsAllDataRequested is the same as Span.IsRecording and will be false when samplers decide to not record the activity, and this can be used to avoid any expensive operation to retrieve tags.
                   //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
                   activity.SetTag("messaging.system", "pulsar").
                            SetTag("messaging.destination_kind", "topic").
                            SetTag("messaging.destination", producer.Topic).
                            SetTag("messaging.operation", "BeforeSend")
                            |> ignore
                   
                   //add propagator here
                   //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageSender.cs#L102
                   let contextToInject = activity.Context;                   
                   Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), message, test);
                    
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
                          activity.SetTag("exception.type", ``exception``.Source). 
                                   SetTag("exception.message", ``exception``.Message). 
                                   SetTag("exception.stacktrace", ``exception``.StackTrace) |> ignore
                          activity.Stop()
                | _ ->
                     let activity = this.activitySource.StartActivity(producer.Topic + " OnSendAcknowledgement",ActivityKind.Producer)                    
                     if activity <> null then               
                        if activity.IsAllDataRequested = true then
                          activity.SetTag("messaging.destination_kind", "topic"). 
                                   SetTag("messaging.destination", producer.Topic). 
                                   SetTag("messaging.operation", "OnSendAcknowledgement"). 
                                   SetTag("messaging.message_id", messageId) |> ignore       
                          activity.Stop()  
          
                   