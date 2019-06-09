namespace Pulsar.Client.Internal

open System.Collections.Concurrent
open Pipelines.Sockets.Unofficial
open System.Net
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

module ConnectionPool =
    let connections = ConcurrentDictionary<DnsEndPoint, Lazy<Task<SocketConnection>>>()

    let rec getConnectionAsync (address: DnsEndPoint) =   
        let connectionTask =
            connections.GetOrAdd(address, fun(address) -> 
                lazy SocketConnection.ConnectAsync(address)).Value
        task {
            let! connection = connectionTask
            if connection.Socket.Connected
            then
                return connection
            else 
                connections.TryRemove(address) |> ignore
                //TODO: try another available address?
                return! getConnectionAsync address
        }
