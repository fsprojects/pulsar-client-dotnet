module Pulsar.Client.UnitTests.Internal.ClientCnxTests

open System
open System.IO.Pipelines
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open System.Timers
open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

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

            // Only now does the send loop finish the first write and reach the second request.
            output.Open()

            let! secondCompleted = completesInTime second
            Expect.isTrue secondCompleted "The second request gets a reply, a ConnectException or a TimeoutException"
        }

        testTask "Calls queued behind Stop, or made after the send loop has stopped, complete" {
            let incoming = Pipe()
            let output = new GatedWriter()
            let cnx = newCnx incoming output
            let sendMb =
                typeof<ClientCnx>.GetProperty("SendMb", BindingFlags.Instance ||| BindingFlags.NonPublic).GetValue(cnx)
                :?> Channel<SocketMessage>

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
    ]
