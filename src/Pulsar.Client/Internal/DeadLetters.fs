namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open FSharp.UMX

type internal DeadLetterProcessor<'T>
    (policy: DeadLetterPolicy,
     getTopicName: unit -> string,
     subscriptionName: string,
     createProducer: string -> Task<IProducer<'T>>) =

    let store = Dictionary<MessageId, Message<'T>>()
    let dlTopicName =
        if String.IsNullOrEmpty(policy.DeadLetterTopic) |> not then
            policy.DeadLetterTopic
        else
            (sprintf "%s-%s%s" (getTopicName()) subscriptionName RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX)

    let dlProducer = lazy (
        createProducer dlTopicName
    )
    
    let rlProducer = lazy (
        createProducer policy.RetryLetterTopic
    )

    interface IDeadLetterProcessor<'T> with
        member this.ClearMessages() =
            store.Clear()

        member this.AddMessage messageId message =
            store.[messageId] <- message

        member this.RemoveMessage messageId =
            store.Remove(messageId) |> ignore

        member this.ProcessMessages messageId acknowledge =
            task {
                match store.TryGetValue messageId with
                | true, message ->
                    Log.Logger.LogInformation("DeadLetter processing topic: {0}, messageId: {1}", dlTopicName, messageId)
                    try
                        let! producer = dlProducer.Value
                        let key =
                            if String.IsNullOrEmpty(%message.Key) then
                                Some { PartitionKey = message.Key; IsBase64Encoded =  message.HasBase64EncodedKey  }
                            else
                                None
                        let msg = MessageBuilder(message.GetValue(), message.Data, key, message.Properties)
                        let! _ = producer.SendAsync(msg)
                        do! acknowledge messageId
                        return true
                    with
                    | ex ->
                        Log.Logger.LogError(ex, "Send to dead letter topic exception with topic: {0}, messageId: {1}", dlTopicName, messageId)
                        return false
                | false, _ ->
                    return false
            }

        member this.MaxRedeliveryCount = policy.MaxRedeliveryCount |> uint32

    static member Disabled = {
        new IDeadLetterProcessor<'T> with
            member this.ClearMessages() = ()
            member this.AddMessage _ _ = ()
            member this.RemoveMessage _ = ()
            member this.ProcessMessages _ _ = Task.FromResult(false)
            member this.MaxRedeliveryCount = UInt32.MaxValue
    }