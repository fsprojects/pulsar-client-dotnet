module Pulsar.Client.UnitTests.Internal.ClientCnxTests

open System
open System.Buffers
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

                use frame = new MemoryStream()
                use writer = new BinaryWriter(frame)
                writer.Write(0)
                let command =
                    BaseCommand(
                        ``type`` = BaseCommand.Type.Message,
                        Message = CommandMessage(
                            ConsumerId = %consumerId,
                            MessageId = MessageIdData(ledgerId = 1UL, entryId = 1UL)))
                Serializer.SerializeWithLengthPrefix(frame, command, PrefixStyle.Fixed32BigEndian)
                writer.Write(int16ToBigEndian MagicNumber)
                let checksumPosition = frame.Position
                writer.Write(0)
                let metadata = MessageMetadata(ProducerName = "test-producer", SequenceId = 1UL, PublishTime = 1UL)
                Serializer.SerializeWithLengthPrefix(frame, metadata, PrefixStyle.Fixed32BigEndian)
                writer.Write([| 42uy |])
                let checksum =
                    ReadOnlySequence<byte>(frame.ToArray().AsMemory(int checksumPosition + 4))
                    |> CRC32C.GetForROS
                    |> int32
                frame.Position <- checksumPosition
                writer.Write(int32ToBigEndian checksum)
                frame.Position <- 0L
                writer.Write(int32ToBigEndian (int frame.Length - 4))
                let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame.ToArray())).AsTask()

                do! disposed.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                Expect.isFalse delivered.Task.IsCompleted "Removed consumers must not receive messages"
            finally
                cnx.Dispose()
                MemoryStreamManager <- originalManager
                subscription.Dispose()
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
    ]
