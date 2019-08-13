module internal Pulsar.Client.Internal.ConnectionPool

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Collections.Concurrent
open System.Net
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open System
open Microsoft.Extensions.Logging
open pulsar.proto
open System.Reflection
open Pulsar.Client.Internal

let clientVersion = "Pulsar.Client v" + Assembly.GetExecutingAssembly().GetName().Version.ToString()
let protocolVersion =
    ProtocolVersion.GetValues(typeof<ProtocolVersion>)
    :?> ProtocolVersion[]
    |> Array.last

let connections = ConcurrentDictionary<LogicalAddress, Lazy<Task<ClientCnx>>>()

let private connect (broker: Broker) =
    Log.Logger.LogInformation("Connecting to {0}", broker)
    task {
        let (PhysicalAddress physicalAddress) = broker.PhysicalAddress
        let (LogicalAddress logicalAddress) = broker.LogicalAddress
        let! socketConnection = SocketConnection.ConnectAsync(physicalAddress)
        let writerStream = StreamConnection.GetWriter(socketConnection.Output)
        let connection = (socketConnection, writerStream)
        let initialConnectionTsc = TaskCompletionSource<ClientCnx>()

        let unregisterClientCnx (broker: Broker) =
            connections.TryRemove(broker.LogicalAddress) |> ignore
        let clientCnx = ClientCnx(broker, connection, initialConnectionTsc, unregisterClientCnx)

        let proxyToBroker = if physicalAddress = logicalAddress then None else Some logicalAddress
        let connectPayload =
            Commands.newConnect clientVersion protocolVersion proxyToBroker
        let! success = clientCnx.Send connectPayload
        if not success then
            raise (ConnectionFailedOnSend "ConnectionPool connect")
        return! initialConnectionTsc.Task
    }

let getConnection (broker: Broker) =
    connections.GetOrAdd(broker.LogicalAddress, fun(address) ->
        lazy connect broker).Value

let getBrokerlessConnection (address: DnsEndPoint) =
    getConnection { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }