namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Collections.Generic
open FSharp.UMX
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive

type internal DeadLettersProcessor
    (policy: DeadLettersPolicy,
     getTopicName: unit -> string,
     getSubscriptionNameName: unit -> string,
     createProducer: string -> Task<IProducer>) =

    let store = Dictionary<MessageId, ResizeArray<Message>>()

    let topicName =
        if String.IsNullOrEmpty(policy.DeadLetterTopic) |> not then
            policy.DeadLetterTopic
        else
            (sprintf "%s-%s-DLQ" (getTopicName()) (getSubscriptionNameName()))

    let producer = lazy (

        createProducer topicName
    )

    let sendMessage (builder : MessageBuilder) messageId  =
        task {
            let! p = producer.Value
            let! _ = p.SendAsync(builder)
            return ()
        }

    let toStoreFormat (messageId : MessageId) =
        { messageId with Type = Individual }

    interface IDeadLettersProcessor with
        member __.ClearMessages() =
            store.Clear()

        member __.AddMessage messageId message =
            if message.RedeliveryCount |> int >= policy.MaxRedeliveryCount then
                let messageId = messageId |> toStoreFormat

                if store.ContainsKey(messageId) |> not then
                    store.[messageId] <- ResizeArray<Message>()

                store.[messageId].Add(message)

        member __.RemoveMessage messageId =
            store.Remove(messageId) |> ignore

        member __.ProcessMessages messageId =
            task {
                let messageId = messageId |> toStoreFormat
                if store.ContainsKey messageId then
                    let messages = store.[messageId]
                    try
                        for message in messages do
                            let mb = MessageBuilder(message.Payload, %message.MessageKey, message.Properties)
                            do! sendMessage mb messageId
                        return true
                    with
                    | ex ->
                        Log.Logger.LogError(
                            ex,
                            "Send to dead letter topic exception with topic: {0}, messageId: {1}",
                            topicName,
                            messageId)
                        return false
                else
                    return false
            }

    static member Disabled = {
        new IDeadLettersProcessor with
            member __.ClearMessages() = ()
            member __.AddMessage _ _ = ()
            member __.RemoveMessage _ = ()
            member __.ProcessMessages _ = Task.FromResult(false)
    }