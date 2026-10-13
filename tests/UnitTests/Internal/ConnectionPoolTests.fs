module Pulsar.Client.UnitTests.Internal.ConnectionPoolTests

open System
open System.Net
open System.Net.Security
open System.Net.Sockets
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal

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

        testTask "SocketFactory ssl stream closes the socket and aborts pending reads on dispose" {
            let! readAborted, socketConnected =
                task {
                    use listener = new TcpListener(IPAddress.Loopback, 0)
                    listener.Start()
                    let port = (listener.LocalEndpoint :?> IPEndPoint).Port
                    use socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                    let acceptTask = listener.AcceptSocketAsync()
                    do! socket.ConnectAsync(IPAddress.Loopback, port)
                    use! _ = acceptTask
                    // the peer stays silent, so the read only completes if the socket gets closed
                    let pendingRead = socket.ReceiveAsync(ArraySegment(Array.zeroCreate<byte> 1), SocketFlags.None)

                    let sslStream = SocketFactory.createSslStream socket (RemoteCertificateValidationCallback(fun _ _ _ _ -> true))
                    sslStream.Dispose()

                    let! completed = Task.WhenAny(pendingRead, Task.Delay(TimeSpan.FromSeconds 5.0))
                    return obj.ReferenceEquals(completed, pendingRead), socket.Connected
                }
            Expect.isTrue "Pending read should be aborted" readAborted
            Expect.isFalse "Socket should be closed" socketConnected
        }
    ]
