namespace Pulsar.Client.Internal

open System.Collections.Concurrent
open Pipelines.Sockets.Unofficial
open System.Net
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common

module ConnectionPool =
    let connections = ConcurrentDictionary<EndPoint, Lazy<Task<SocketConnection>>>()

    let rec getConnectionAsync (broker: Broker) =   
        let connectionTask =
            connections.GetOrAdd(broker.PhysicalAddress, fun(address) -> 
                lazy SocketConnection.ConnectAsync(address)).Value
        task {
            let! connection = connectionTask
            if connection.Socket.Connected
            then
                return connection
            else 
                connections.TryRemove(broker.PhysicalAddress) |> ignore
                //TODO: try another available address?
                return! getConnectionAsync broker
        }
