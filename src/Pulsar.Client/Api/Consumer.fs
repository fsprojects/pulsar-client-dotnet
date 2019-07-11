namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Concurrent
open System
open Pulsar.Client.Internal
open System.Runtime.CompilerServices
open Pulsar.Client.Common
open pulsar.proto
open Microsoft.Extensions.Logging

type ConsumerException(message) =
    inherit Exception(message)

type ConsumerStatus =
    | Normal
    | Terminated

type ConsumerState = {
    Status: ConsumerStatus
    WaitingChannel: AsyncReplyChannel<Message>
}

type Consumer private (consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) as this =

    let consumerId = Generators.getNextConsumerId()
    let queue = new ConcurrentQueue<Message>()
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<Message>>

    let registerConsumer() =
        task {
            let! broker = lookup.GetBroker(consumerConfig.Topic)
            let! clientCnx = ConnectionPool.getConnection broker
            do! clientCnx.RegisterConsumer consumerConfig consumerId this.Mb
            return clientCnx
        } |> Async.AwaitTask

    let connectionHandler = ConnectionHandler registerConsumer

    let mb = MailboxProcessor<ConsumerMessage>.Start(fun inbox ->

        let rec loop (state: ConsumerState) =
            async {
                let! msg = inbox.Receive()
                if (state.Status <> ConsumerStatus.Normal)
                then
                    failwith (sprintf "Consumer status: %A" state.Status)
                else
                    match msg with
                    | ConsumerMessage.Connect channel ->
                        do! connectionHandler.Connect()
                        channel.Reply()
                        return! loop state
                    | ConsumerMessage.ConnectionClosed ->
                        do! connectionHandler.ConnectionClosed()
                        return! loop state
                    | ConsumerMessage.MessageRecieved x ->
                        if state.WaitingChannel = nullChannel
                        then
                            queue.Enqueue(x)
                            return! loop state
                        else
                            state.WaitingChannel.Reply(x)
                            return! loop { state with WaitingChannel = nullChannel }
                    | ConsumerMessage.GetMessage ch ->
                        match queue.TryDequeue() with
                        | true, msg ->
                            ch.Reply msg
                            return! loop state
                        | false, _ ->
                            return! loop { state with WaitingChannel = ch }
                    | ConsumerMessage.Ack (payload, channel) ->
                        match connectionHandler.ConnectionState with
                        | Ready conn ->
                            do! conn.Send payload
                            channel.Reply()
                            return! loop state
                        | _ ->
                            //TODO put message on schedule
                            return! loop state
                    | ConsumerMessage.ReachedEndOfTheTopic ->
                        return! loop { state with Status = Terminated }
            }
        loop { Status = Normal; WaitingChannel = nullChannel}
    )

    member __.ReceiveAsync() =
        task {
            match queue.TryDequeue() with
            | true, msg ->
                return msg
            | false, _ ->
                 return! mb.PostAndAsyncReply(GetMessage)
        }

    member __.AcknowledgeAsync (msg: Message) =
        task {
            let command = Commands.newAck consumerId msg.MessageId CommandAck.AckType.Individual
            do! mb.PostAndAsyncReply(fun channel -> Ack (command, channel))
            return! Task.FromResult()
        }

    member private __.InitInternal() =
        task {
            let! broker = lookup.GetBroker(consumerConfig.Topic)
            return! mb.PostAndAsyncReply(Connect)
        }

    member private __.Mb with get() = mb

    static member Init(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =
        task {
            let consumer = Consumer(consumerConfig, lookup)
            do! consumer.InitInternal()
            return consumer
        }



