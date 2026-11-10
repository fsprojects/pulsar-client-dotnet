module Pulsar.Client.UnitTests.Internal.LocalBroker

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading.Tasks
open ProtoBuf
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal.ClientCnxTests

/// A broker on a local socket, for what takes its connections from the connection pool. It accepts every connection
/// and subscription, and answers a consumer's close only once released. The other commands are left to the handler
/// the test gives it, which gets the command and the function that sends an answer.
type internal LocalBroker(handle: BaseCommand -> (BaseCommand -> unit) -> unit) =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    do listener.Start()
    let endpoint = DnsEndPoint("127.0.0.1", (listener.LocalEndpoint :?> IPEndPoint).Port)
    let gate = obj()
    let mutable holdCloses = true
    let heldCloses = ResizeArray<unit -> unit>()
    let closeRequested = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let clients = ResizeArray<TcpClient>()

    let readCommand (stream: Stream) =
        task {
            let sizeBytes = Array.zeroCreate<byte> 4
            do! stream.ReadExactlyAsync(sizeBytes.AsMemory())
            let frame = Array.zeroCreate<byte> (BitConverter.ToInt32(sizeBytes) |> int32FromBigEndian)
            do! stream.ReadExactlyAsync(frame.AsMemory())
            let commandSize = BitConverter.ToInt32(frame, 0) |> int32FromBigEndian
            use commandStream = new MemoryStream(frame, 4, commandSize)
            return Serializer.Deserialize<BaseCommand>(commandStream)
        }

    let serve (client: TcpClient) =
        task {
            use client = client
            lock clients (fun () -> clients.Add client)
            let stream = client.GetStream()
            let send (command: BaseCommand) =
                let frame = frameOf command
                lock stream (fun () -> stream.Write(frame, 0, frame.Length))
            let succeed requestId =
                send (BaseCommand(``type`` = BaseCommand.Type.Success, Success = CommandSuccess(RequestId = requestId)))
            try
                while true do
                    let! (command: BaseCommand) = readCommand stream
                    match command.``type`` with
                    | BaseCommand.Type.Connect ->
                        send (BaseCommand(
                                ``type`` = BaseCommand.Type.Connected,
                                Connected = CommandConnected(
                                    ServerVersion = "test-broker",
                                    ProtocolVersion = int ProtocolVersion.V19,
                                    MaxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE)))
                    | BaseCommand.Type.Subscribe ->
                        succeed command.Subscribe.RequestId
                    | BaseCommand.Type.CloseConsumer ->
                        let requestId = command.CloseConsumer.RequestId
                        let answerNow =
                            lock gate (fun () ->
                                if holdCloses then
                                    heldCloses.Add(fun () -> succeed requestId)
                                not holdCloses)
                        closeRequested.TrySetResult() |> ignore
                        if answerNow then
                            succeed requestId
                    | BaseCommand.Type.Ping ->
                        send (BaseCommand(``type`` = BaseCommand.Type.Pong, Pong = CommandPong()))
                    | _ ->
                        handle command send
            with _ ->
                // the client closed the connection
                ()
        }

    do
        task {
            try
                while true do
                    let! client = listener.AcceptTcpClientAsync()
                    serve client |> ignore
            with _ ->
                // the listener was stopped
                ()
        } |> ignore

    new() = new LocalBroker(fun _ _ -> ())

    member _.Broker = { LogicalAddress = LogicalAddress endpoint; PhysicalAddress = PhysicalAddress endpoint }

    /// Completes when a consumer has asked to be closed.
    member _.CloseRequested = closeRequested.Task

    /// Answers the closes held so far, and every later one at once.
    member _.ReleaseCloses() =
        let held =
            lock gate (fun () ->
                holdCloses <- false
                let held = heldCloses.ToArray()
                heldCloses.Clear()
                held)
        for answer in held do
            answer ()

    /// Closes the connections the broker has, as a broker that goes away does. It accepts new ones.
    member _.DropConnections() =
        let dropped =
            lock clients (fun () ->
                let dropped = clients.ToArray()
                clients.Clear()
                dropped)
        for client in dropped do
            client.Close()

    interface IDisposable with
        member _.Dispose() = listener.Stop()

/// Sends every connection to the broker, and expects no other lookup.
let internal lookupFor (broker: LocalBroker) =
    { new ILookupService with
        member _.GetBroker _ = Task.FromResult broker.Broker
        member _.GetPartitionsForTopic _ = failwith "Unexpected partition lookup"
        member _.GetPartitionedTopicMetadata _ = failwith "Unexpected metadata lookup"
        member _.GetTopicsUnderNamespace(_, _) = failwith "Unexpected namespace lookup"
        member _.GetSchema(_, ?schema) = failwith "Unexpected schema lookup"
        member _.UpdateServiceInfo _ = failwith "Unexpected service update"
        member _.Dispose() = () }
