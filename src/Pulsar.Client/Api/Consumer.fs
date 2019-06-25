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

type ConsumerException(message) =
    inherit Exception(message)

type ConsumerState = {
    Connection: ConnectionState
}

type Consumer(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =    

    let consumerId = Generators.getNextConsumerId()
    let queue = new ConcurrentQueue<Message>()
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<Message>>  

    let mb = MailboxProcessor<ConsumerMessage>.Start(fun inbox ->
        let mutable channel: AsyncReplyChannel<Message> = nullChannel
        let rec loop (state: ConsumerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ConsumerMessage.Connect ((broker, mb), channel) ->
                    let! connection = SocketManager.registerConsumer broker consumerId mb |> Async.AwaitTask
                    channel.Reply()
                    return! loop { state with Connection = Connected connection }  
                | ConsumerMessage.Reconnect ->
                    // TODO backoff
                    let topicName = TopicName(consumerConfig.Topic)
                    let! broker = lookup.GetBroker(topicName) |> Async.AwaitTask
                    let! connection = SocketManager.getConnection broker |> Async.AwaitTask
                    return! loop { state with Connection = Connected connection }
                | ConsumerMessage.Disconnected (connection, mb) ->
                    if state.Connection = Connected connection
                    then
                        mb.Post(ConsumerMessage.Reconnect)
                        return! loop { state with Connection = NotConnected }
                    else 
                        return! loop state
                | ConsumerMessage.AddMessage x ->
                    if channel = nullChannel
                    then 
                        queue.Enqueue(x)
                    else 
                        channel.Reply(x)
                        channel <- nullChannel
                    return! loop state
                | ConsumerMessage.GetMessage ch ->
                    match queue.TryDequeue() with
                    | true, msg ->
                        ch.Reply msg
                    | false, _ ->
                        channel <- ch                    
                    return! loop state
                | ConsumerMessage.Ack (payload, channel) ->
                    match state.Connection with
                    | Connected conn ->
                        do! SocketManager.send (conn, payload)
                        channel.Reply()
                        return! loop state
                    | NotConnected ->
                        //TODO put message on schedule
                        return! loop state                             
            }
        loop { Connection = NotConnected }
    )    

    member this.ReceiveAsync() =
        task {
            match queue.TryDequeue() with
            | true, msg ->
                return msg
            | false, _ ->
                 return! mb.PostAndAsyncReply(GetMessage)           
        }

    member this.AcknowledgeAsync (msg: Message) =       
        task {
            let command = Commands.newAck consumerId %msg.MessageId.LedgerId %msg.MessageId.EntryId CommandAck.AckType.Individual
            do! mb.PostAndAsyncReply(fun channel -> Ack (command, channel))
            return! Task.FromResult()
        }

    member private __.InitInternal() =
        task {
            let topicName = TopicName(consumerConfig.Topic)
            let! broker = lookup.GetBroker(topicName)
            return! mb.PostAndAsyncReply(fun channel -> Connect ((broker, mb), channel))
        }

    static member Init(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =
        task {
            let consumer = Consumer(consumerConfig, lookup)
            do! consumer.InitInternal()
            return consumer
        }
       

        
