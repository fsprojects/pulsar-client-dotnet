module Pulsar.Client.Internal.SocketManager

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Collections.Concurrent
open System.Net
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial


let connections = ConcurrentDictionary<EndPoint, Lazy<Task<SocketConnection>>>()

let rec getConnection (broker: Broker) =   
    connections.GetOrAdd(broker.PhysicalAddress, fun(address) -> 
        lazy SocketConnection.ConnectAsync(address)).Value

