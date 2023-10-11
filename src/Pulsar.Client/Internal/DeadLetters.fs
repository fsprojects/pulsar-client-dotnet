namespace Pulsar.Client.Internal

open System
open System.Collections.Generic

open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Threading.Tasks
open Microsoft.Extensions.Logging
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

        member this.ProcessMessage (messageId, acknowledge) =
            match store.TryGetValue messageId with
            | true, message ->
                Log.Logger.LogInformation("DeadLetter processing topic: {0}, messageId: {1}", dlTopicName, messageId)
                let key = getOptionalKey message
                let value = Unchecked.defaultof<'T> // no data decoding is needed
                let msg = MessageBuilder(value, message.Data, key, message.Properties)
                backgroundTask {
                    let! producer = dlProducer.Value
                    return
                        backgroundTask {
                            let! _ = producer.SendAsync(msg)
                            try
                                do! acknowledge messageId
                                return true
                            with Flatten ex ->
                                Log.Logger.LogWarning(ex, "Failed to acknowledge the message topic: {0}, messageId: {1} but send to the DLQ successfully",
                                                      dlTopicName, messageId)
                                return true
                        }
                }
            | false, _ ->
                falseTaskTask

        member this.ReconsumeLater (message, deliverAt, acknowledge) =
            let propertiesMap = Dictionary<string, string>()
            for KeyValue(k, v) in message.Properties do
                propertiesMap.[k] <- v
            let mutable reconsumetimes = 1
            match propertiesMap.TryGetValue(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES) with
            | true, v ->
                reconsumetimes <- v |> int |> (+) 1
            | _ ->
                propertiesMap.[RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC] <- topicName
                propertiesMap.[RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID] <- message.MessageId.ToString()
            propertiesMap.[RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES] <- string reconsumetimes
            propertiesMap.[RetryMessageUtil.SYSTEM_PROPERTY_DELIVER_AT] <- deliverAt |> string
            backgroundTask {
                if reconsumetimes > policy.MaxRedeliveryCount then
                    let dlp = this :> IDeadLetterProcessor<'T>
                    dlp.AddMessage(message.MessageId, message.WithProperties(propertiesMap))
                    let! _ = dlp.ProcessMessage(message.MessageId, acknowledge)
                    return ()
                else
                    let! rlProducer = rlProducer.Value
                    let key = getOptionalKey message
                    let eventTime = message.EventTime |> Option.ofNullable
                    let msg = MessageBuilder(message.GetValue(), message.Data, key, propertiesMap, deliverAt, ?eventTime = eventTime)
                    let! _ = rlProducer.SendAsync(msg)
                    do! acknowledge message.MessageId
            }

        member this.MaxRedeliveryCount = policy.MaxRedeliveryCount
        member this.TopicName = dlTopicName

    static member Disabled = {
        new IDeadLetterProcessor<'T> with
            member this.ClearMessages () = ()
            member this.AddMessage (_,_) = ()
            member this.RemoveMessage _ = ()
            member this.ProcessMessage (_,_) = falseTaskTask
            member this.MaxRedeliveryCount = Int32.MaxValue
            member this.TopicName = ""
            member this.ReconsumeLater (_,_,_) = unitTask
    }