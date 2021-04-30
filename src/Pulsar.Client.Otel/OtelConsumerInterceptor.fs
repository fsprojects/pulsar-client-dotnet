module Pulsar.Client.Otel.OtelConsumerInterceptor

open System
open System.Collections.Generic
open System.Diagnostics
open System.Linq
open OpenTelemetry
open OpenTelemetry.Context.Propagation
open Pulsar.Client.Api
open Pulsar.Client.Common

  type AcknowledgeType =
    | Timeout of (string*MessageId)
    | Ok of (string*MessageId)
    | Cumulative of (string*MessageId)
    | Negative of (string*MessageId)
    | Stop

type OTelConsumerInterceptor<'T>() =
    let cache =  Dictionary<MessageId, Activity>()
    static let source = "pulsar.consumer"  
    let activitySource : ActivitySource = new ActivitySource(source)
    let Propagator = Propagators.DefaultTextMapPropagator
    let createActivity (topicName:string) =
            activitySource.StartActivity(topicName + " consumed", ActivityKind.Consumer)
    let workWithException (topicName:string,exn:Exception)=
            let activity = createActivity(topicName)
            activity.SetTag("exception.type", exn.Source)
                    .SetTag("exception.message", exn.Message)
                    .SetTag("exception.stacktrace", exn.StackTrace)
                    |> ignore
            activity.Stop()          
    
    let getter =
        Func<IReadOnlyDictionary<string, string>, string, IEnumerable<string>>
            (fun dict key ->
                match dict.TryGetValue(key) with
                | true, v -> [ v ] :> IEnumerable<string>
                | false, _ -> Enumerable.Empty<string>())

   
    
    let mb =  MailboxProcessor<AcknowledgeType>.Start(fun inbox ->              
                
                let attachOkTagsAndStop (act: Activity,ackType:string, topicName:string, messageID:MessageId) =
                    act.SetTag("acknowledge.type", ackType)
                       .SetTag("messaging.destination_kind", "topic")
                       .SetTag("messaging.destination", topicName)
                       .SetTag("messaging.message_id", messageID)
                       .SetTag("messaging.operation", "AfterConsume")
                       |> ignore
                    act.Stop()
                let stopAllPrevAndCurrent (ackType:string, topicName:string, messageID:MessageId) =
                    cache
                    |> Seq.filter (fun a -> messageID > a.Key)
                    |> Seq.iter (fun a -> attachOkTagsAndStop (a.Value, ackType, topicName, messageID))
                    
                let stopActFromCache(ackType:string, topicName:string, messageID:MessageId) =                     
                    attachOkTagsAndStop (cache.[messageID], ackType, topicName, messageID)
                    
                let gotActFromCache(messageID:MessageId) = cache.ContainsKey(messageID)
                
                let stopCachedOrStopNew(ackType:string,msgId:MessageId, topicName:string)=
                      let cached = gotActFromCache(msgId)
                      if cached then stopActFromCache(ackType,topicName,msgId)
                      else attachOkTagsAndStop (cache.[msgId], ackType, topicName, msgId)
                      
                let rec messageLoop() = async{
                        
                        let! msg = inbox.Receive()                           
                        match msg with
                        | AcknowledgeType.Ok (topicName,msgId) ->                            
                            stopCachedOrStopNew("Ok",msgId,topicName)
                        | AcknowledgeType.Negative (topicName,msgId) ->                           
                            stopCachedOrStopNew("Negative",msgId,topicName)
                        | AcknowledgeType.Timeout (topicName,msgId) ->                         
                            stopCachedOrStopNew("Timeout",msgId,topicName)
                        | AcknowledgeType.Cumulative (topicName,msgId) ->
                            stopAllPrevAndCurrent("Cumulative",topicName,msgId)
                            stopCachedOrStopNew("Cumulative",msgId,topicName)                          
                        | AcknowledgeType.Stop ->
                             cache |> Seq.iter (fun a ->  a.Value.SetTag("messaging.operation", "StoppingOnClose")
                                                          |> ignore
                                                          a.Value.Stop())
                        return! messageLoop() 
                        }       
                messageLoop()
        )
    
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
                    cache.Add(message.MessageId, activity)                   
            message


        member this.Close() =
            mb.Post <| AcknowledgeType.Stop
            activitySource.Dispose()
            cache.Clear()            
            ()

        member this.OnAckTimeoutSend(consumer, messageId) =
            mb.Post <| AcknowledgeType.Timeout (consumer.Topic,messageId)            

        member this.OnAcknowledge(consumer, messageId, exn) =
            match exn with
            | null -> mb.Post <| AcknowledgeType.Ok (consumer.Topic,messageId)
            | _ -> workWithException(consumer.Topic,exn)

        member this.OnAcknowledgeCumulative(consumer, messageId,exn) =
             match exn with
             | null -> mb.Post <| AcknowledgeType.Cumulative (consumer.Topic,messageId)            
             | _ -> workWithException(consumer.Topic,exn)
        member this.OnNegativeAcksSend(consumer, messageId) =
            mb.Post <| AcknowledgeType.Negative (consumer.Topic,messageId)
            
