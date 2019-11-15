namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Collections.Generic
open FSharp.Control.Tasks.V2.ContextInsensitive
open FSharp.UMX
open System.Threading.Tasks

type internal DeadLettersProcessor
    (policy: DeadLettersPolicy,
     getTopicName: unit -> string,
     getSubscriptionNameName: unit -> string,
     createProducer: string -> Task<IProducer>) =

    let store = Dictionary<MessageId, ResizeArray<Message>>()

    let producer = lazy (

        let topicName =
            if String.IsNullOrEmpty(policy.DeadLetterTopic) |> not then
                policy.DeadLetterTopic
            else
                (sprintf "%s-%s-DLQ" (getTopicName()) (getSubscriptionNameName()))

        createProducer topicName
    )

    let sendAndForgetAsync (builder : MessageBuilder) =
        task {
            let! p = producer.Value
            p.SendAsync(builder) |> ignore
            return ()
        }

    interface IDeadLettersProcessor with
        member __.ClearMessages() =
            store.Clear()

        member __.AddMessage (message : Message) =
            let id = message.MessageId
            if message.RedeliveryCount |> int >= policy.MaxRedeliveryCount then
                if store.ContainsKey(id) |> not then store.[id] <- ResizeArray<Message>()
                store.[id].Add(message)

        member __.RemoveMessage messageId =
            store.Remove(messageId) |> ignore

        member this.ProcessMessages messageId =
            if  store.ContainsKey messageId then

                store.[messageId]
                |> Seq.map (fun m -> MessageBuilder(m.Payload, %m.MessageKey, m.Properties))
                |> Seq.iter (fun builder -> sendAndForgetAsync(builder) |> ignore)

                messageId |> (this :> IDeadLettersProcessor).RemoveMessage
                true
            else
                false

    static member Disabled = {
        new IDeadLettersProcessor with
            member __.ClearMessages() = ()
            member __.AddMessage _ = ()
            member __.RemoveMessage _ = ()
            member __.ProcessMessages _ = false
    }