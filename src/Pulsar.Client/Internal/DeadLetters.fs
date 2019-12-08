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

    let sendMessage (builder : MessageBuilder) =
        task {
            let! p = producer.Value
            let! _ = p.SendAsync(builder)
            return ()
        }

    interface IDeadLettersProcessor with
        member this.ClearMessages() =
            store.Clear()

        member this.AddMessage messageId messages =
            store.[messageId] <- messages

        member this.RemoveMessage messageId =
            store.Remove(messageId) |> ignore

        member this.ProcessMessages messageId acknowledge =
            task {
                match store.TryGetValue messageId with
                | true, messages ->
                    Log.Logger.LogInformation("DeadLetter processing topic: {0}, messageId: {1}, messagesCount: {2}", topicName, messageId, messages.Count)
                    try
                        for message in messages do
                            let mb = MessageBuilder(message.Payload, %message.MessageKey, message.Properties)
                            do! sendMessage mb
                        do! acknowledge()
                        return true
                    with
                    | ex ->
                        Log.Logger.LogError(ex, "Send to dead letter topic exception with topic: {0}, messageId: {1}", topicName, messageId)
                        return false
                | false, _ ->
                    return false
            }

        member this.MaxRedeliveryCount = policy.MaxRedeliveryCount |> uint32

    static member Disabled = {
        new IDeadLettersProcessor with
            member this.ClearMessages() = ()
            member this.AddMessage _ _ = ()
            member this.RemoveMessage _ = ()
            member this.ProcessMessages _ _ = Task.FromResult(false)
            member this.MaxRedeliveryCount = UInt32.MaxValue
    }