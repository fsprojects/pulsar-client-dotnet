namespace Pulsar.Client.Internal

open System
open System.Buffers.Binary
open System.Collections.Generic
open Newtonsoft.Json.Converters
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open FSharp.UMX

type internal DeadLetterProcessor<'T>
    (policy: DeadLetterPolicy,
     getTopicName: unit -> string,
     subscriptionName: SubscriptionName,
     createProducer: string -> Task<IProducer<'T>>) =

    let topicName = getTopicName()
    let store = Dictionary<MessageId, Message<'T>>()
    let dlTopicName =
        if String.IsNullOrEmpty(policy.DeadLetterTopic) |> not then
            policy.DeadLetterTopic
        else
            $"{topicName}-{subscriptionName}{RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX}"

    let dlProducer = lazy (
        createProducer dlTopicName
    )
    
    let rlProducer = lazy (
        createProducer policy.RetryLetterTopic
    )
    
    let getOptionalKey (message: Message<'T>) = 
            if String.IsNullOrEmpty(%message.Key) then
                Some { PartitionKey = message.Key; IsBase64Encoded =  message.HasBase64EncodedKey  }
            else
                None

    interface IDeadLetterProcessor<'T> with
        member this.ClearMessages() =
            store.Clear()

        member this.AddMessage (messageId, message) =
            store.[messageId] <- message

        member this.RemoveMessage messageId =
            store.Remove(messageId) |> ignore

        member this.ProcessMessages (messageId, acknowledge) =
            task {
                match store.TryGetValue messageId with
                | true, message ->
                    Log.Logger.LogInformation("DeadLetter processing topic: {0}, messageId: {1}", dlTopicName, messageId)
                    try
                        let! producer = dlProducer.Value
                        let key = getOptionalKey message
                        let value = Unchecked.defaultof<'T> // no data decoding is needed
                        let msg = MessageBuilder(value, message.Data, key, message.Properties)
                        let! _ = producer.SendAsync(msg)
                        do! acknowledge messageId
                        return true
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "Send to dead letter topic exception with topic: {0}, messageId: {1}", dlTopicName, messageId)
                        return false
                | false, _ ->
                    return false
            }

        member this.ReconsumeLater (message, deliverAt, acknowledge) =
            let propertiesMap = Dictionary<string, string>()
            for KeyValue(k, v) in message.Properties do
                propertiesMap.Add(k, v)
            let mutable reconsumetimes = 1
            match propertiesMap.TryGetValue(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES) with
            | true, v ->
                reconsumetimes <- v |> int |> (+) 1
            | _ ->
                propertiesMap.Add(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC, topicName)
                propertiesMap.Add(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID, message.MessageId.ToString())
            propertiesMap.Add(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES, string reconsumetimes)
            propertiesMap.Add(RetryMessageUtil.SYSTEM_PROPERTY_DELIVER_AT, deliverAt |> convertToMsTimestamp |> string)
            task {
                if reconsumetimes > policy.MaxRedeliveryCount then
                    let dlp = this :> IDeadLetterProcessor<'T>
                    dlp.AddMessage(message.MessageId, message.WithProperties(propertiesMap))
                    let! dlResult = dlp.ProcessMessages(message.MessageId, acknowledge)
                    return dlResult
                else
                    try
                        let! rlProducer = rlProducer.Value
                        let key = getOptionalKey message
                        let msg = MessageBuilder(message.GetValue(), message.Data, key, propertiesMap, deliverAt)
                        let! _ = rlProducer.SendAsync(msg)
                        do! acknowledge message.MessageId
                        return true
                    with Flatten ex ->
                        Log.Logger.LogError(ex, "Send to retry topic exception with topic: {0}, messageId: {1}",
                                            dlTopicName, message.MessageId)
                        return false
            }
        
        member this.MaxRedeliveryCount = policy.MaxRedeliveryCount |> uint32

    static member Disabled = {
        new IDeadLetterProcessor<'T> with
            member this.ClearMessages () = ()
            member this.AddMessage (_,_) = ()
            member this.RemoveMessage _ = ()
            member this.ProcessMessages (_,_) = Task.FromResult(false)
            member this.MaxRedeliveryCount = UInt32.MaxValue
            member this.ReconsumeLater (_,_,_) = Task.FromResult(false)
    }