module Pulsar.Client.UnitTests.Internal.ConnectionPoolTests

open System.Net
open System.Net.Sockets
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal

[<Tests>]
let tests =
    testList "ConnectionPool" [
        test "SocketFactory creates dual-mode IPv6 socket for unspecified endpoints" {
            let endpoint = DnsEndPoint("my-broker", 6650)
            use socket = SocketFactory.createSocket endpoint

            Expect.equal "" socket.AddressFamily AddressFamily.InterNetworkV6
            Expect.equal "" socket.DualMode true
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
    ]
