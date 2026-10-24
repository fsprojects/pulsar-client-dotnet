module Pulsar.Client.UnitTests.Internal.HttpLookupServiceTests

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

let private testTimeout = TimeSpan.FromSeconds(10.0)

// Answers every request on one connection with 404, as the broker does for the lookup of a deleted topic
let private serveNotFound (listener: TcpListener) =
    backgroundTask {
        use! client = listener.AcceptTcpClientAsync()
        use stream = client.GetStream()
        use reader = new StreamReader(stream, Encoding.ASCII, false, 1024, true)
        let mutable line = ""
        let mutable answered = false
        while not answered do
            line <- reader.ReadLine()
            match line with
            | null -> answered <- true
            | "" ->
                let response = Encoding.ASCII.GetBytes("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                do! stream.WriteAsync(response, 0, response.Length)
                answered <- true
            | _ -> ()
    }

[<Tests>]
let tests =
    testList "HttpLookupService" [
        testTask "A lookup that is not found fails with NotFoundException" {
            let listener = new TcpListener(IPAddress.Loopback, 0)
            listener.Start()
            try
                let port = (listener.LocalEndpoint :?> IPEndPoint).Port
                let serving = serveNotFound listener
                let config =
                    { PulsarClientConfiguration.Default with
                        ServiceAddresses = [| Uri($"http://127.0.0.1:{port}") |] }
                let lookup = new HttpLookupService(config) :> ILookupService
                let topic = TopicName("persistent://public/default/deleted-topic").CompleteTopicName
                // as in the Java client, so a producer or consumer whose topic was deleted stops instead of reconnecting
                let! (ex: exn) =
                    task {
                        try
                            try
                                let! _ = lookup.GetBroker(topic).WaitAsync(testTimeout)
                                return null
                            with Flatten ex ->
                                return ex
                        finally
                            lookup.Dispose()
                    }
                Expect.isTrue (ex :? NotFoundException) $"Expected NotFoundException, got {ex}"
                Expect.isTrue (PulsarClientException.isUnrecoverableError ex) "A deleted topic is unrecoverable"
                do! serving.WaitAsync(testTimeout)
            finally
                listener.Stop()
        }
    ]
