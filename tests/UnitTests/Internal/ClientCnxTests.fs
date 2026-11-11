module Pulsar.Client.UnitTests.Internal.ClientCnxTests

open System
open System.Buffers
open System.Buffers.Binary
open System.IO
open System.IO.Pipelines
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open System.Timers
open Expecto
open FSharp.UMX
open Microsoft.IO
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal.ConnectionHandlerTests
open pulsar.proto

let private config =
    { PulsarClientConfiguration.Default with
        OperationTimeout = TimeSpan.FromMilliseconds(200.0)
        KeepAliveInterval = TimeSpan.FromMinutes(1.0) }

let private broker =
    let endpoint = DnsEndPoint("localhost", 6650)
    { LogicalAddress = LogicalAddress endpoint; PhysicalAddress = PhysicalAddress endpoint }

let private topic = TopicName("public/default/topic").CompleteTopicName

/// A socket output whose flushes wait until the test opens the gate, so the send loop can be held inside a write.
type private GatedWriter() =
    inherit PipeWriter()
    let inner = Pipe().Writer
    let entered = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    let gate = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    member _.FlushEntered = entered.Task
    member _.Open() = gate.TrySetResult() |> ignore
    override _.Advance(bytes) = inner.Advance(bytes)
    override _.GetMemory(sizeHint) = inner.GetMemory(sizeHint)
    override _.GetSpan(sizeHint) = inner.GetSpan(sizeHint)
    override _.CancelPendingFlush() = inner.CancelPendingFlush()
    override _.Complete(ex) = inner.Complete(ex)
    override _.FlushAsync(cancellationToken: CancellationToken) =
        entered.TrySetResult() |> ignore
        ValueTask<FlushResult>(task {
            do! gate.Task
            return! inner.FlushAsync(cancellationToken).AsTask()
        })

let private newCnx (incoming: Pipe) (output: PipeWriter) =
    ClientCnx(config, broker,
        { Input = incoming.Reader; Output = output; Dispose = fun () -> incoming.Writer.Complete() },
        Commands.DEFAULT_MAX_MESSAGE_SIZE,
        TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously),
        ignore)

/// The bytes a broker sends for a command: [total size][command size][command].
let internal frameOf (command: BaseCommand) =
    use stream = new MemoryStream()
    stream.Write(Array.zeroCreate<byte> 4, 0, 4)
    Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)
    let frame = stream.ToArray()
    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), frame.Length - 4)
    frame

let private serialize (value: 'T) =
    use stream = new MemoryStream()
    Serializer.Serialize(stream, value)
    stream.ToArray()

/// The bytes a broker sends for a message:
/// [total size][command size][command][magic number][checksum][metadata size][metadata][payload].
let private messageFrameOf (consumerId: ConsumerId) (metadata: byte[]) (payload: byte[]) =
    let command =
        BaseCommand(
            ``type`` = BaseCommand.Type.Message,
            Message = CommandMessage(
                ConsumerId = %consumerId,
                MessageId = MessageIdData(ledgerId = 1UL, entryId = 1UL)))
    use stream = new MemoryStream()
    stream.Write(Array.zeroCreate<byte> 4, 0, 4)
    Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)
    let magicPosition = int stream.Position
    stream.Write(Array.zeroCreate<byte> 10, 0, 10)
    stream.Write(metadata, 0, metadata.Length)
    stream.Write(payload, 0, payload.Length)
    let frame = stream.ToArray()
    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), frame.Length - 4)
    BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(magicPosition, 2), MagicNumber)
    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(magicPosition + 6, 4), metadata.Length)
    let checksum = ReadOnlySequence<byte>(frame.AsMemory(magicPosition + 6)) |> CRC32C.GetForROS |> int32
    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(magicPosition + 2, 4), checksum)
    frame

let private validMetadata () =
    serialize (MessageMetadata(ProducerName = "test-producer", SequenceId = 1UL, PublishTime = 1UL))

/// A consumer on the connection, with what the connection told it.
type private RegisteredConsumer(cnx: ClientCnx, consumerId: ConsumerId) =
    let received = TaskCompletionSource<RawMessage>(TaskCreationOptions.RunContinuationsAsynchronously)
    let closed = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    do cnx.AddConsumer(consumerId, {
        MessageReceived = fun struct(message, _) ->
            received.TrySetResult message |> ignore
            message.Payload.Dispose()
        ReachedEndOfTheTopic = ignore
        ActiveConsumerChanged = ignore
        ConnectionClosed = fun _ -> closed.TrySetResult() |> ignore
        AckReceipt = ignore
        AckError = ignore
    })
    member _.ConsumerId = consumerId
    member _.Received = received.Task
    member _.ConnectionClosed = closed.Task

/// Consumers are added through the operations mailbox, so the reply to a later request there means they are registered.
let private registered (cnx: ClientCnx) =
    cnx.RemoveConsumerAndWait(%UInt64.MaxValue).WaitAsync(TimeSpan.FromSeconds(5.0))

let private request (cnx: ClientCnx) =
    let requestId = Generators.getNextRequestId()
    cnx.SendAndWaitForReply requestId (Commands.newPartitionMetadataRequest topic requestId)

let private mailbox<'T> name (cnx: ClientCnx) =
    typeof<ClientCnx>.GetProperty(name, BindingFlags.Instance ||| BindingFlags.NonPublic).GetValue(cnx) :?> Channel<'T>

/// Holds the requests loop until the returned gate is opened: the loop completes an injected request whose continuation
/// runs on the loop's thread and waits there.
let private holdRequestsLoop (requestsMb: Channel<RequestsOperation>) : Task<TaskCompletionSource> =
    task {
        let held = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        let gate = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        let reply = TaskCompletionSource<PulsarResponseType>()
        reply.Task.ContinueWith((fun (_: Task<PulsarResponseType>) ->
            held.TrySetResult() |> ignore
            gate.Task.Wait(TimeSpan.FromSeconds(5.0)) |> ignore), TaskContinuationOptions.ExecuteSynchronously) |> ignore
        let requestId = Generators.getNextRequestId()
        // Written from the thread pool: requestsMb allows synchronous continuations, so its loop can run on the
        // writer's thread, and that thread is the one held.
        Task.Run(fun () ->
            requestsMb.Writer.TryWrite(AddRequest(requestId, BaseCommand.Type.PartitionedMetadata, reply)) |> ignore
            requestsMb.Writer.TryWrite(CompleteRequest(requestId, BaseCommand.Type.PartitionedMetadata, PulsarResponseType.Empty)) |> ignore)
        |> ignore
        do! held.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
        return gate
    }

// requestsMb stops its timeout timer and operationsMb its keep-alive timer when they process Stop,
// so both timers being disabled means both mailboxes have stopped.
let private mailboxesStopped (cnx: ClientCnx) =
    task {
        let timers =
            typeof<ClientCnx>.GetFields(BindingFlags.Instance ||| BindingFlags.NonPublic)
            |> Array.choose (fun field ->
                match field.GetValue(cnx) with
                | :? Timer as timer -> Some timer
                | _ -> None)
        Expect.equal timers.Length 2 "ClientCnx has a request-timeout timer and a keep-alive timer"
        let deadline = DateTime.UtcNow.AddSeconds(5.0)
        while timers |> Array.exists _.Enabled && DateTime.UtcNow < deadline do
            do! Task.Delay(10)
        Expect.isFalse (timers |> Array.exists _.Enabled) "The mailboxes stop after the connection is deactivated"
    }

// Waits well past OperationTimeout: a request must have a reply, a ConnectException or a TimeoutException by then.
let private completesInTime (request: Task) =
    task {
        let! finished = Task.WhenAny(request, Task.Delay(TimeSpan.FromSeconds(2.0)))
        return finished = request
    }

let private failsWithConnectException (request: Task<PulsarResponseType>) description =
    task {
        let! completed = completesInTime request
        Expect.isTrue completed $"{description} completes"
        Expect.isTrue request.IsFaulted $"{description} fails"
        let ex = request.Exception.InnerException
        Expect.isTrue (ex :? ConnectException) $"{description} fails with ConnectException, got {ex}"
    }

let private undisposable (incoming: Pipe) (config: PulsarClientConfiguration) =
    ClientCnx(config, broker,
        { Input = incoming.Reader; Output = Pipe().Writer; Dispose = fun () -> failwith "could not dispose" },
        Commands.DEFAULT_MAX_MESSAGE_SIZE,
        TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously),
        ignore)

[<Tests>]
let tests =
    testList "ClientCnx" [
        testSequenced <| testTask "Messages arriving after consumer removal dispose pooled payloads" {
            let originalManager = MemoryStreamManager
            let manager = RecyclableMemoryStreamManager()
            let disposed = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let subscription = manager.StreamDisposed.Subscribe(fun args ->
                if args.Tag = "payload" then
                    disposed.TrySetResult() |> ignore)
            let incoming = Pipe()
            let outgoing = Pipe()
            let cnx = newCnx incoming outgoing.Writer
            let delivered = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let consumerId = %1UL
            try
                // This test runs alone because the stream manager is shared by all connections.
                MemoryStreamManager <- manager
                cnx.AddConsumer(consumerId, {
                    MessageReceived = fun struct(message, _) ->
                        delivered.TrySetResult() |> ignore
                        message.Payload.Dispose()
                    ReachedEndOfTheTopic = ignore
                    ActiveConsumerChanged = ignore
                    ConnectionClosed = ignore
                    AckReceipt = ignore
                    AckError = ignore
                })
                do! cnx.RemoveConsumerAndWait(consumerId).WaitAsync(TimeSpan.FromSeconds(5.0))

                let frame = messageFrameOf consumerId (validMetadata ()) [| 42uy |]
                let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame)).AsTask()

                do! disposed.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                Expect.isFalse delivered.Task.IsCompleted "Removed consumers must not receive messages"
            finally
                cnx.Dispose()
                MemoryStreamManager <- originalManager
                subscription.Dispose()
        }

        testTask "A message that fails its checksum reaches its consumer as corrupted" {
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let consumer = RegisteredConsumer(cnx, %1UL)
            do! registered cnx
            let frame = messageFrameOf consumer.ConsumerId (validMetadata ()) [| 42uy |]
            frame[frame.Length - 1] <- 43uy
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame)).AsTask()

            // as in the Java client, the consumer discards it and tells the broker
            let! (message: RawMessage) = consumer.Received.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse message.CheckSumValid "The message is marked as corrupted"
            Expect.isTrue cnx.IsActive "A corrupted message leaves the connection open"
            cnx.Dispose()
        }

        testTask "A message whose metadata can't be parsed reaches its consumer as corrupted" {
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let consumer = RegisteredConsumer(cnx, %1UL)
            let nextConsumer = RegisteredConsumer(cnx, %2UL)
            do! registered cnx
            // a string field that claims more bytes than the metadata has
            let frame = messageFrameOf consumer.ConsumerId [| 0x0Auy; 0x64uy |] [| 42uy |]
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame)).AsTask()

            // as in the Java client, instead of the frame being skipped with its permit never returned
            let! (message: RawMessage) = consumer.Received.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse message.CheckSumValid "The message is marked as corrupted"
            Expect.equal message.Metadata.NumMessages 1 "A corrupted message counts as one message"

            // the connection stays open and reads the next frame
            let next = messageFrameOf nextConsumer.ConsumerId (validMetadata ()) [| 42uy |]
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(next)).AsTask()
            let! (nextMessage: RawMessage) = nextConsumer.Received.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isTrue nextMessage.CheckSumValid "The next message is delivered as usual"
            Expect.isTrue cnx.IsActive "A corrupted message leaves the connection open"
            cnx.Dispose()
        }

        testSequenced <| testTask "A message that can't be read for any other reason closes the connection" {
            let originalManager = MemoryStreamManager
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let consumer = RegisteredConsumer(cnx, %1UL)
            try
                do! registered cnx
                // This test runs alone because the stream manager is shared by all connections. No payload fits it,
                // as when the process is out of memory.
                MemoryStreamManager <- RecyclableMemoryStreamManager(RecyclableMemoryStreamManager.Options(MaximumStreamCapacity = 1L))
                let frame = messageFrameOf consumer.ConsumerId (validMetadata ()) (Array.zeroCreate 16)
                let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame)).AsTask()

                // As in the Java client, the consumer reconnects and the broker sends the message again. Skipping the
                // frame would lose the message and its permit while the consumer stays connected.
                do! consumer.ConnectionClosed.WaitAsync(TimeSpan.FromSeconds(5.0))
                Expect.isFalse cnx.IsActive "The connection is deactivated"
                Expect.isFalse consumer.Received.IsCompleted "The message was not delivered"
            finally
                cnx.Dispose()
                MemoryStreamManager <- originalManager
        }

        testTask "A request the send loop reaches after the connection is deactivated still completes" {
            let incoming = Pipe()
            let output = new GatedWriter()
            let cnx = newCnx incoming output

            // The send loop takes the first request and is held inside its write; the second waits behind it.
            let first = request cnx
            do! output.FlushEntered.WaitAsync(TimeSpan.FromSeconds(5.0))
            let second = request cnx

            // The broker closes the connection: it is deactivated and its mailboxes stop.
            incoming.Writer.Complete()
            do! mailboxesStopped cnx
            Expect.isFalse cnx.IsActive "The connection is deactivated"
            let! firstCompleted = completesInTime first
            Expect.isTrue firstCompleted "The request registered before the close fails with the connection"
            // Wait until requestsMb is completed too, so the second request's AddRequest is rejected; the last test covers
            // one queued behind Stop. Not asserted: without the change nothing completes requestsMb, and the second
            // request is what fails.
            let! _ = completesInTime (mailbox<RequestsOperation> "RequestsMb" cnx).Reader.Completion

            // Only now does the send loop finish the first write and reach the second request.
            output.Open()

            let! secondCompleted = completesInTime second
            Expect.isTrue secondCompleted "The second request gets a reply, a ConnectException or a TimeoutException"
        }

        testTask "Calls queued behind Stop, or made after the send loop has stopped, complete" {
            let incoming = Pipe()
            let output = new GatedWriter()
            let cnx = newCnx incoming output
            let sendMb = mailbox<SocketMessage> "SendMb" cnx

            // The send loop is held inside a write, and Stop is queued behind it while the connection is still active,
            // so the next calls pass the IsActive check and land behind Stop.
            let _held = request cnx
            do! output.FlushEntered.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isTrue (sendMb.Writer.TryWrite SocketMessage.Stop) "Stop is queued"
            let queuedSend = cnx.Send(Commands.newPing())
            let queuedRequest = request cnx

            output.Open()

            let! queuedSendCompleted = completesInTime queuedSend
            Expect.isTrue queuedSendCompleted "A send queued behind Stop completes"
            Expect.isFalse queuedSend.Result "A send queued behind Stop reports that nothing was sent"
            do! failsWithConnectException queuedRequest "A request queued behind Stop"

            // The send loop has stopped and its mailbox is completed, but the connection still counts as active.
            Expect.isTrue cnx.IsActive "The connection was never deactivated"
            let lateSend = cnx.Send(Commands.newPing())
            let lateRequest = request cnx
            let! lateSendCompleted = completesInTime lateSend
            Expect.isTrue lateSendCompleted "A send after the send loop stopped completes"
            Expect.isFalse lateSend.Result "A send after the send loop stopped reports that nothing was sent"
            do! failsWithConnectException lateRequest "A request after the send loop stopped"
        }

        testTask "A request whose AddRequest is queued behind Stop completes" {
            let incoming = Pipe()
            let outgoing = Pipe()
            let cnx = newCnx incoming outgoing.Writer
            let requestsMb = mailbox<RequestsOperation> "RequestsMb" cnx

            // With the requests loop held, Stop is queued, and then a request whose AddRequest lands behind it.
            let! (gate: TaskCompletionSource) = holdRequestsLoop requestsMb
            try
                Expect.isTrue (requestsMb.Writer.TryWrite RequestsOperation.Stop) "Stop is queued"
                let queued = request cnx
                // The send loop writes a request only after requestsMb has accepted its AddRequest.
                let! written = completesInTime (outgoing.Reader.ReadAsync().AsTask())
                Expect.isTrue written "The send loop wrote the request, so requestsMb accepted its AddRequest"

                gate.TrySetResult() |> ignore

                do! failsWithConnectException queued "A request queued behind Stop in requestsMb"
            finally
                // Also releases the requests loop when an assertion above fails.
                gate.TrySetResult() |> ignore
        }

        testTask "A frame is read as soon as its last bytes arrive, even when they are fewer than 8" {
            // The writer pauses until the reader has examined what was written, so the reader sees the start of the
            // frame on its own, as when a TLS record or a pipe buffer boundary splits the frame.
            let incoming = Pipe(PipeOptions(pauseWriterThreshold = 1L, resumeWriterThreshold = 1L))
            let connected = TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously)
            let _cnx =
                ClientCnx(config, broker,
                    { Input = incoming.Reader; Output = Pipe().Writer; Dispose = fun () -> incoming.Writer.Complete() },
                    Commands.DEFAULT_MAX_MESSAGE_SIZE, connected, ignore)
            let frame =
                frameOf (BaseCommand(``type`` = BaseCommand.Type.Connected,
                                     Connected = CommandConnected(ServerVersion = "test-broker", ProtocolVersion = int ProtocolVersion.V19)))
            let split = frame.Length - 3
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame, 0, split)).AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame, split, 3)).AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))

            let! _ = connected.Task.WaitAsync(TimeSpan.FromSeconds(2.0))
            incoming.Writer.Complete()
        }

        testTask "Registering a consumer again replaces its earlier registration" {
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let first = RegisteredConsumer(cnx, %1UL)
            let second = RegisteredConsumer(cnx, %1UL)
            // as in the Java client, and the connection goes on looking after its consumers
            do! registered cnx
            let frame = messageFrameOf %1UL (validMetadata ()) [| 42uy |]
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame)).AsTask()

            let! (_: RawMessage) = second.Received.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse first.Received.IsCompleted "The earlier registration was replaced"
            Expect.isTrue cnx.IsActive "The connection stays open"
            cnx.Dispose()
        }

        testTask "A closed connection tells every consumer, whatever fails on the way" {
            let incoming = Pipe()
            let cnx = undisposable incoming config
            cnx.AddConsumer(%1UL, {
                MessageReceived = fun struct(message, _) -> message.Payload.Dispose()
                ReachedEndOfTheTopic = ignore
                ActiveConsumerChanged = ignore
                ConnectionClosed = fun _ -> failwith "the consumer could not be told"
                AckReceipt = ignore
                AckError = ignore
            })
            let consumer = RegisteredConsumer(cnx, %2UL)
            do! registered cnx
            let pending = request cnx

            // the broker closes the connection
            incoming.Writer.Complete()

            // the consumer reconnects only if it learns that, and the requests are answered by the other mailboxes
            do! consumer.ConnectionClosed.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse cnx.IsActive "The connection is deactivated"
            do! mailboxesStopped cnx
            do! failsWithConnectException pending "A pending request"
        }

        testTask "A connection that can't be disposed after a keep-alive timeout is still deactivated" {
            let incoming = Pipe()
            let cnx = undisposable incoming { config with KeepAliveInterval = TimeSpan.FromMilliseconds(50.0) }
            let consumer = RegisteredConsumer(cnx, %1UL)

            // The broker never answers the ping. As in the Java client, what fails while the connection is looked
            // after closes it, so its consumers reconnect instead of staying on a connection nobody looks after.
            do! consumer.ConnectionClosed.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.isFalse cnx.IsActive "The connection is deactivated"
            do! mailboxesStopped cnx
        }

        // sequenced: it replaces the logger that everything shares
        testSequenced <| testTask "A removal is answered although the connection fails while it deals with it" {
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let held = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let gate = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let original = Log.Logger
            try
                Log.Logger <-
                    FailingLogger(original, fun message ->
                        if message.Contains "adding consumer 1" then
                            // Holds the loop, so the removal waits in the mailbox. A removal the loop takes as it
                            // is posted is dealt with before its caller looks at the connection once more.
                            held.TrySetResult() |> ignore
                            gate.Task.Wait(TimeSpan.FromSeconds(5.0)) |> ignore
                            false
                        else
                            // as anything else that can fail where the removal is dealt with
                            message.Contains "consumer 2")
                // from another thread, as the loop can run on the thread that posts to it
                Task.Run(fun () -> RegisteredConsumer(cnx, %1UL) |> ignore) |> ignore
                do! held.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                let removal = cnx.RemoveConsumerAndWait(%2UL)
                gate.SetResult()

                do! removal.WaitAsync(TimeSpan.FromSeconds(5.0))
            finally
                gate.TrySetResult() |> ignore
                Log.Logger <- original
                if cnx.IsActive then
                    cnx.Dispose()
        }

        // sequenced: it replaces the logger that everything shares
        testSequenced <| testTask "A connection is deactivated when a cancellation stops the loop that looks after it" {
            let incoming = Pipe()
            let cnx = newCnx incoming (Pipe().Writer)
            let consumer = RegisteredConsumer(cnx, %1UL)
            do! registered cnx
            let original = Log.Logger
            try
                // the removal fails where it is logged, and so does dealing with that failure
                Log.Logger <-
                    cancellingLogger original (fun message ->
                        message.Contains "removed consumer 2" || message.Contains "operation failed")
                cnx.RemoveConsumer(%2UL)

                // As when the loop ends faulted. Its consumers would otherwise stay on a connection that nothing
                // looks after any more.
                do! consumer.ConnectionClosed.WaitAsync(TimeSpan.FromSeconds(5.0))
                Expect.isFalse cnx.IsActive "The connection is deactivated"
            finally
                Log.Logger <- original
                if cnx.IsActive then
                    cnx.Dispose()
        }
    ]
