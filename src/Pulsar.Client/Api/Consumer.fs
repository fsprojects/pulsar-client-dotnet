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
    Connection: ConnectionState
    Status: ConsumerStatus
    WaitingChannel: AsyncReplyChannel<Message>
    ReconnectCount: int
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
        }

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
                        if state.Connection = NotConnected
                        then
                            let! clientCnx = registerConsumer() |> Async.AwaitTask
                            channel.Reply()
                            return! loop { state with Connection = Connected clientCnx }
                        else
                            return! loop state
                    | ConsumerMessage.Reconnect ->
                        // TODO backoff
                        if state.Connection = NotConnected
                        then
                            try
                                let! clientCnx = registerConsumer() |> Async.AwaitTask
                                return! loop { state with Connection = Connected clientCnx; ReconnectCount = 0 }
                            with
                            | ex ->
                                this.Mb.Post(ConsumerMessage.Reconnect)
                                Log.Logger.LogError(ex, "Error reconnecting")
                                if state.ReconnectCount > 3
                                then
                                    raise ex
                                else
                                    return! loop { state with ReconnectCount = state.ReconnectCount + 1 }
                        else
                            return! loop state
                    | ConsumerMessage.Disconnected ->
                        this.Mb.Post(ConsumerMessage.Reconnect)
                        return! loop { state with Connection = NotConnected }
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
                        match state.Connection with
                        | Connected conn ->
                            do! conn.Send payload
                            channel.Reply()
                            return! loop state
                        | NotConnected ->
                            //TODO put message on schedule
                            return! loop state
                    | ConsumerMessage.ReachedEndOfTheTopic ->
                        return! loop { state with Status = Terminated }
            }
        loop { Connection = NotConnected; Status = Normal; WaitingChannel = nullChannel; ReconnectCount = 0 }
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



