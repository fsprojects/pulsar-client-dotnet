namespace Pulsar.Client.Internal

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
open System.IO.Pipelines
open Pulsar.Client.Api

type ConnectionPool (config: PulsarClientConfiguration) =

    let clientVersion = "Pulsar.Client v" + Assembly.GetExecutingAssembly().GetName().Version.ToString()
    let protocolVersion =
        ProtocolVersion.GetValues(typeof<ProtocolVersion>)
        :?> ProtocolVersion[]
        |> Array.last

    let connections = ConcurrentDictionary<LogicalAddress, Lazy<Task<ClientCnx>>>()

    let connect (broker: Broker) =
        Log.Logger.LogInformation("Connecting to {0}", broker)
        task {
            let (PhysicalAddress physicalAddress) = broker.PhysicalAddress
            let (LogicalAddress logicalAddress) = broker.LogicalAddress
            let! socketConnection = SocketConnection.ConnectAsync(physicalAddress, PipeOptions(pauseWriterThreshold = 5_242_880L ))
            let writerStream = StreamConnection.GetWriter(socketConnection.Output)
            let connection = (socketConnection, writerStream)
            let initialConnectionTsc = TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously)
            let unregisterClientCnx (broker: Broker) =
                connections.TryRemove(broker.LogicalAddress) |> ignore
            let clientCnx = ClientCnx(config, broker, connection, initialConnectionTsc, unregisterClientCnx)
            let proxyToBroker = if physicalAddress = logicalAddress then None else Some logicalAddress
            let authenticationDataProvider = config.Authentication.GetAuthData(physicalAddress.Host);
            let authData = authenticationDataProvider.Authenticate({ Bytes = AuthData.INIT_AUTH_DATA })
            let authMethodName = config.Authentication.GetAuthMethodName()
            let connectPayload = Commands.newConnect authMethodName authData clientVersion protocolVersion proxyToBroker

            let! success = clientCnx.Send connectPayload
            if not success then
                raise (ConnectionFailedOnSend "ConnectionPool connect")
            return! initialConnectionTsc.Task
        }

    member this.GetConnection (broker: Broker) =
        connections.GetOrAdd(broker.LogicalAddress, fun(address) ->
            lazy connect broker).Value

    member this.GetBrokerlessConnection (address: DnsEndPoint) =
        this.GetConnection { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }

    member this.Close() =
        connections
        |> Seq.map (fun kv -> kv.Value.Value.Result)
        |> Seq.iter (fun clientCnx -> clientCnx.Dispose())