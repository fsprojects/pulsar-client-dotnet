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

type MailboxMessage =
    | AddMessage of Message
    | GetMessage of AsyncReplyChannel<Message>

type Consumer(consumerConfig: ConsumerConfiguration, lookup: BinaryLookupService) =    

    let consumerId = Generators.getNextConsumerId()
    let queue = new ConcurrentQueue<Message>()
    let connectionHandler = ConnectionHandler()    
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<Message>>
    let mutable clientCnx: ClientCnx option = None

    do connectionHandler.ConnectionOpened.Add(fun conn -> 
        clientCnx <- Some { Connection = conn; ProducerId = %0L; ConsumerId = consumerId }
    )

    let mb = MailboxProcessor.Start(fun inbox ->
        let mutable channel: AsyncReplyChannel<Message> = nullChannel
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | AddMessage x ->
                    if channel = nullChannel
                    then 
                        queue.Enqueue(x)
                    else 
                        channel.Reply(x)
                        channel <- nullChannel
                | GetMessage ch ->
                    match queue.TryDequeue() with
                    | true, msg ->
                        ch.Reply msg
                    | false, _ ->
                        channel <- ch
                return! loop ()             
            }
        loop ()
    )    

    do connectionHandler.MessageReceived.Add(fun msg -> 
        mb.Post(AddMessage msg)
    )

    do connectionHandler.GrabCnx consumerConfig.Topic lookup |> ignore

    member this.ReceiveAsync() =
        task {
            match queue.TryDequeue() with
            | true, msg ->
                return msg
            | false, _ ->
                 return! mb.PostAndAsyncReply(GetMessage)           
        }

    member this.AcknowledgeAsync (msg: Message) =
        if clientCnx.IsNone
        then failwith "Connection is not ready"
        else
            task {
                let command = 
                           Commands.newAck consumerId msg.MessageId.LedgerId msg.MessageId.EntryId CommandAck.AckType.Individual
                           |> ReadOnlyMemory<byte>
                let! flushResult = clientCnx.Value.Connection.Output.WriteAsync(command)
                return! Task.FromResult()
            }
       

        
