namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Collections.Generic
open FSharp.UMX
open System.Threading.Tasks
open FSharp.Control.Tasks
open Microsoft.Extensions.Logging

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

    let sendMessageAsync (builder : MessageBuilder) messageId  =
        task {
            try
                let! p = producer.Value
                do! (p.SendAsync(builder) :> Task)
                return true
            with
            | ex ->
                Log.Logger.LogError(
                    ex,
                    "Send to dead letter topic exception with topic: {0}, messageId: {1}",
                    topicName,
                    messageId)
                return false
        } |> Async.AwaitTask

    let toStoreFormat (messageId : MessageId) = {
        LedgerId = messageId.LedgerId
        EntryId = messageId.EntryId
        Partition = messageId.Partition
        Type = MessageIdType.Individual
        TopicName = %""
    }

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
            let messageId = messageId |> toStoreFormat
            if store.ContainsKey messageId then
                store.[messageId]
                |> Seq.map (fun m -> MessageBuilder(m.Payload, %m.MessageKey, m.Properties))
                |> Seq.map (fun builder -> sendMessageAsync builder messageId)
                |> Async.Parallel
                |> Async.RunSynchronously
                |> Array.contains false
                |> not 
            else
                false

    static member Disabled = {
        new IDeadLettersProcessor with
            member __.ClearMessages() = ()
            member __.AddMessage _ _ = ()
            member __.RemoveMessage _ = ()
            member __.ProcessMessages _ = false
    }