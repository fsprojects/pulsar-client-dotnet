module Pulsar.Client.UnitTests.Internal.ConnectionPoolTests

open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal.LocalBroker

/// Its first request for authentication data is cancelled, like a token request that times out.
type private AuthenticationCancelledOnce() =
    inherit Authentication()
    let mutable calls = 0
    override _.GetAuthMethodName() = "none"
    override _.GetAuthData() =
        if Interlocked.Increment(&calls) = 1 then
            raise (OperationCanceledException "the token request timed out")
        AuthenticationDataProvider()

[<Tests>]
let tests =
    testList "ConnectionPool" [
        test "SocketFactory creates dual-mode IPv6 socket for unspecified endpoints when IPv6 is supported" {
            let endpoint = DnsEndPoint("my-broker", 6650)
            use socket = SocketFactory.createSocket endpoint

            // The production code only creates a dual-mode IPv6 socket when the OS supports
            // IPv6, otherwise it falls back to an IPv4 socket. Branch the assertions the same
            // way so the test stays portable on hosts/containers with IPv6 disabled.
            if Socket.OSSupportsIPv6 then
                Expect.equal "" socket.AddressFamily AddressFamily.InterNetworkV6
                Expect.equal "" socket.DualMode true
            else
                Expect.equal "" socket.AddressFamily AddressFamily.InterNetwork

            Expect.equal "" socket.SocketType SocketType.Stream
            Expect.equal "" socket.ProtocolType ProtocolType.Tcp
        }

        test "SocketFactory keeps explicit IPv4 endpoint socket family" {
            let endpoint = DnsEndPoint("127.0.0.1", 6650, AddressFamily.InterNetwork)
            use socket = SocketFactory.createSocket endpoint

            Expect.equal "" socket.AddressFamily AddressFamily.InterNetwork
            Expect.equal "" socket.ProtocolType ProtocolType.Tcp
        }

        test "SocketFactory keeps explicit IPv6 endpoint socket family" {
            let endpoint = DnsEndPoint("::1", 6650, AddressFamily.InterNetworkV6)
            use socket = SocketFactory.createSocket endpoint

            Expect.equal "" socket.AddressFamily AddressFamily.InterNetworkV6
            Expect.equal "" socket.ProtocolType ProtocolType.Tcp
        }

        testTask "A connection attempt that ended cancelled is not kept" {
            let timeout = TimeSpan.FromSeconds(10.0)
            let broker = new LocalBroker()
            let config =
                { PulsarClientConfiguration.Default with
                    Authentication = new AuthenticationCancelledOnce()
                    KeepAliveInterval = TimeSpan.FromMinutes(1.0) }
            let connectionPool = ConnectionPool(config)
            let connect () = connectionPool.GetConnection(broker.Broker, Commands.DEFAULT_MAX_MESSAGE_SIZE)
            try
                let cancelled = connect ()
                try
                    let! (_: ClientCnx) = cancelled.WaitAsync(timeout)
                    ()
                with _ ->
                    ()
                Expect.isTrue "The attempt ends cancelled, not faulted" cancelled.IsCanceled

                // As in the Java client, the pool lets go of an attempt that didn't succeed. It does that when it is
                // asked for the connection: here, or already above, if the attempt ended while it was being started.
                // A cancelled attempt would stay for good otherwise.
                connect () |> ignore
                let! (cnx: ClientCnx) = (connect ()).WaitAsync(timeout)
                Expect.isTrue "The pool has connected again" cnx.IsActive
            finally
                connectionPool.CloseAllConnections().Wait(timeout) |> ignore
                (broker :> IDisposable).Dispose()
        }
    ]
