module Pulsar.Client.Internal.SocketManager

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Collections.Concurrent
open System.Net
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open System
open System.IO.Pipelines


let connections = ConcurrentDictionary<EndPoint, Lazy<Task<SocketConnection>>>()

let rec getConnection (broker: Broker) =   
    connections.GetOrAdd(broker.PhysicalAddress, fun(address) -> 
        lazy SocketConnection.ConnectAsync(address)).Value


type Payload = SocketConnection*ReadOnlyMemory<byte>
type SocketMessage = Payload * AsyncReplyChannel<FlushResult>

let mb = MailboxProcessor<SocketMessage>.Start(fun inbox ->
    let rec loop () =
        async {
            let! ((connection, payload), replyChannel) = inbox.Receive()
            let! flushResult = connection.Output.WriteAsync(payload).AsTask() |> Async.AwaitTask
            replyChannel.Reply(flushResult)
            return! loop ()             
        }
    loop ()
) 

let send payload = mb.PostAndAsyncReply(fun replyChannel -> payload, replyChannel)
