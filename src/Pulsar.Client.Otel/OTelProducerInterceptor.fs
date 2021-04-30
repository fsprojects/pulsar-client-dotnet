module Pulsar.Client.Otel.OTelProducerInterceptor

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open OpenTelemetry
open Pulsar.Client.Api
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Common

//https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Api/README.md#instrumenting-a-libraryapplication-with-net-activity-api
type OTelProducerInterceptor<'T>() =
    let Propagator = Propagators.DefaultTextMapPropagator    
    let setter = Action<Dictionary<string,string>,string,string>(fun msg key value -> msg.Add(key,value))
    static let  source = "pulsar.producer"
    static let activityKey = "traceparent"
    let activitySource = new ActivitySource(source)    
    let cache = ConcurrentDictionary<string,Activity>()
    static member Source
        with get() = source
    interface IProducerInterceptor<'T> with        
        member this.BeforeSend(producer, message) =            
            let mutableDict =  Dictionary<string,string>(message.Properties.Count)
            message.Properties |> Seq.iter (fun k -> mutableDict.Add(k.Key,k.Value))
            
            let name = producer.Topic + " send"            
            let activity = activitySource.StartActivity(name,ActivityKind.Producer)
            
            
            if activity = null then  message    //If there are no listeners interested in this activity, the activity above will be null <..> Ensure that all subsequent calls using this activity are protected with a null check.                
                else               
                if activity.IsAllDataRequested = true then // It is highly recommended to check activity.IsAllDataRequested,
                                                           // before populating any tags which are not readily available.
                                                           // IsAllDataRequested is the same as Span.IsRecording and will be false
                                                           // when samplers decide to not record the activity,
                                                           // and this can be used to avoid any expensive operation to retrieve tags.
                   //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
                   //https://github.com/open-telemetry/opentelemetry-dotnet/blob/a25741030f05c60c85be102ce7c33f3899290d49/examples/MicroserviceExample/Utils/Messaging/MessageSender.cs#L102
                   let contextToInject = activity.Context                   
                   Propagator.Inject(PropagationContext(contextToInject, Baggage.Current), mutableDict, setter)
                   let m  = message.WithProperties(mutableDict)                   
                   cache.TryAdd(mutableDict.[activityKey],activity) |> ignore
                   m
                else
                   cache.TryAdd(mutableDict.[activityKey],activity) |> ignore
                   message   
        member this.Close() =
            activitySource.Dispose()
            cache.Clear()
            () 
        member this.Eligible _ = true
        member this.OnSendAcknowledgement(producer, builder, messageId, exn) =           
                let AddTagsAndStop (act:Activity) =
                    act.SetTag("messaging.destination_kind", "topic") 
                       .SetTag("messaging.destination", producer.Topic) 
                       .SetTag("messaging.operation", "OnSendAcknowledgement") //because we already set it in beforeSend
                       .SetTag("messaging.message_id", messageId) |> ignore       
                    act.Stop()  
                match exn with
                | null ->                      
                     let prevActivity = cache.TryGetValue(builder.Properties.[activityKey])                     
                     match (fst prevActivity) with
                     | true -> AddTagsAndStop (snd prevActivity)
                     |_ ->  let activity = activitySource.StartActivity(producer.Topic + " OnSendAcknowledgement",ActivityKind.Producer)                    
                            if activity <> null then               
                               if activity.IsAllDataRequested = true then
                                  AddTagsAndStop activity
                |  _ ->
                      let activity = activitySource.StartActivity("exception",ActivityKind.Producer)                      
                     //https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md                                             
                      activity.SetTag("exception.type", exn.Source) 
                              .SetTag("exception.message", exn.Message) 
                              .SetTag("exception.stacktrace", exn.StackTrace) |> ignore
                      activity.Stop()
                
          
                   