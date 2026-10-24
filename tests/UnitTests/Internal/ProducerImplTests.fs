module Pulsar.Client.UnitTests.Internal.ProducerImplTests

open System
open System.IO
open System.IO.Pipelines
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Tasks
open System.Timers
open Expecto
open FSharp.UMX
open ProtoBuf
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

let private testTimeout = TimeSpan.FromSeconds(10.0)

// The stopped-mailbox drain consumes ticks, so mailbox size cannot prove that timers stopped.
let private getTimers (instance: obj) =
    instance.GetType().GetFields(BindingFlags.Instance ||| BindingFlags.NonPublic)
    |> Array.choose (fun field ->
        match field.GetValue(instance) with
        | :? Timer as timer -> Some timer
        | _ -> None)

let private expectFailure<'T when 'T :> exn> (operation: Task) =
    task {
        try
            do! operation.WaitAsync(testTimeout)
            return failtest "Expected the operation to fail"
        with Flatten ex ->
            Expect.isTrue operation.IsFaulted "The operation must fail rather than reach the test timeout"
            match ex with
            | :? 'T as expected -> return expected
            | _ -> return failtestf "Expected %s, got %O" typeof<'T>.Name ex
    }

type private BrokerConnection(clientConfig: PulsarClientConfiguration) =
    let incoming = Pipe()
    let outgoing = Pipe()
    let output = outgoing.Reader.AsStream()
    let mutable disconnected = 0
    let producerTimers = ResizeArray<Timer>()
    let connected = TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously)
    let endpoint = DnsEndPoint("localhost", 6650)
    let broker = { LogicalAddress = LogicalAddress endpoint; PhysicalAddress = PhysicalAddress endpoint }
    let disconnect() =
        if Interlocked.Exchange(&disconnected, 1) = 0 then
            incoming.Writer.Complete()
    let connection =
        ClientCnx(clientConfig, broker,
            { Input = incoming.Reader; Output = outgoing.Writer; Dispose = disconnect },
            Commands.DEFAULT_MAX_MESSAGE_SIZE, connected, ignore)
    let lookup =
        { new ILookupService with
            member _.GetBroker _ = Task.FromResult broker
            member _.GetPartitionsForTopic _ = failwith "Unexpected partition lookup"
            member _.GetPartitionedTopicMetadata _ = failwith "Unexpected metadata lookup"
            member _.GetTopicsUnderNamespace(_, _) = failwith "Unexpected namespace lookup"
            member _.GetSchema(_, ?schema) = failwith "Unexpected schema lookup"
            member _.UpdateServiceInfo _ = failwith "Unexpected service update"
            member _.Dispose() = () }

    member _.Connection = connection
    member _.Disconnect() = disconnect()

    member _.ReadCommand() =
        task {
            use cancellation = new CancellationTokenSource(testTimeout)
            let sizeBytes = Array.zeroCreate<byte> 4
            do! output.ReadExactlyAsync(sizeBytes.AsMemory(), cancellation.Token)
            let size = BitConverter.ToInt32(sizeBytes) |> int32FromBigEndian
            let frame = Array.zeroCreate<byte> size
            do! output.ReadExactlyAsync(frame.AsMemory(), cancellation.Token)
            let commandSize = BitConverter.ToInt32(frame, 0) |> int32FromBigEndian
            use commandStream = new MemoryStream(frame, 4, commandSize)
            return Serializer.Deserialize<BaseCommand>(commandStream)
        }

    member _.Reply(command: BaseCommand) =
        task {
            use frame = new MemoryStream()
            use writer = new BinaryWriter(frame)
            writer.Write(0)
            Serializer.SerializeWithLengthPrefix(frame, command, PrefixStyle.Fixed32BigEndian)
            let size = int frame.Length - 4
            frame.Position <- 0L
            writer.Write(int32ToBigEndian size)
            let! _ = incoming.Writer.WriteAsync(ReadOnlyMemory(frame.ToArray())).AsTask().WaitAsync(testTimeout)
            return ()
        }

    member this.StartProducer(config, interceptors, cleanup) =
        task {
            let! sent = connection.Send(connection.NewConnectCommand()).WaitAsync(testTimeout)
            Expect.isTrue sent "The connection handshake should be sent"
            let! connect = this.ReadCommand()
            Expect.equal connect.``type`` BaseCommand.Type.Connect "Expected connection handshake"
            do! this.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Connected,
                    Connected = CommandConnected(
                        ServerVersion = "test-broker",
                        ProtocolVersion = int ProtocolVersion.V19,
                        MaxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE)))
            let! _ = connected.Task.WaitAsync(testTimeout)
            let starting =
                ProducerImpl.Init(config, clientConfig, (fun _ -> Task.FromResult connection),
                                  -1, lookup, Schema.BYTES(), interceptors, cleanup)
            let! command = this.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.Producer "Expected producer registration"
            do! this.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.ProducerSuccess,
                    ProducerSuccess = CommandProducerSuccess(
                        RequestId = command.Producer.RequestId,
                        ProducerName = "test-producer",
                        LastSequenceId = -1L)))
            let! producer = starting.WaitAsync(testTimeout)
            producerTimers.AddRange(getTimers producer)
            return producer
        }

    member this.RejectClose() =
        task {
            let! command = this.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "Expected close request"
            do! this.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Error,
                    Error = CommandError(
                        RequestId = command.CloseProducer.RequestId,
                        Error = ServerError.PersistenceError,
                        Message = "Close rejected")))
        }

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            for timer in producerTimers do
                timer.Dispose()
            connection.Dispose()
            for timer in getTimers connection do
                timer.Dispose()
            output.Dispose()
            incoming.Reader.Complete()
            outgoing.Writer.Complete()
            lookup.Dispose()
            ValueTask()

type private RecordingInterceptor() =
    let events = ResizeArray<string>()
    let disposed = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    member _.Events = lock events (fun () -> List.ofSeq events)
    member _.Disposed = disposed.Task
    interface IProducerInterceptor<byte[]> with
        member _.Eligible _ = true
        member _.BeforeSend(_, message) = message
        member _.OnSendAcknowledgement(_, _, _, exn) =
            if not (isNull exn) then
                lock events (fun () -> events.Add "failed")
        member _.Dispose() =
            lock events (fun () -> events.Add "disposed")
            disposed.TrySetResult() |> ignore

let private clientConfig =
    { PulsarClientConfiguration.Default with
        StatsInterval = TimeSpan.FromMinutes(1.0)
        KeepAliveInterval = TimeSpan.FromMinutes(1.0) }

let private producerConfig =
    { ProducerConfiguration.Default with
        Topic = TopicName("public/default/producer-close") }

let private enqueueMessage (producer: ProducerImpl<byte[]>) payload =
    let message = (producer :> IProducer<byte[]>).NewMessage(payload)
    postAndAsyncReply producer.Mb (fun reply -> BeginSendMessage(struct(message, reply, false)))

let private expectTimersStopped producer =
    let timers = getTimers producer
    Expect.equal timers.Length 4 "Expected all four producer timers"
    for timer in timers do
        Expect.isFalse timer.Enabled "Producer timers must stop before disposal completes"

[<Tests>]
let tests =
    testList "ProducerImpl" [
        testTask "Mailbox failure does not write disposed payloads to the shared connection" {
            use broker = new BrokerConnection(clientConfig)
            let config = { producerConfig with BatchingEnabled = false; SendTimeout = TimeSpan.Zero }
            let! producer = broker.StartProducer(config, ProducerInterceptors<byte[]>.Empty, ignore)
            let writing = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let write _ =
                writing.TrySetResult() |> ignore
                release.Task :> Task
            let blockedWrite = broker.Connection.Send(struct(write, BaseCommand.Type.Ping))
            do! writing.Task.WaitAsync(testTimeout)

            try
                let sending = enqueueMessage producer [| 1uy |]
                let crashing =
                    postAndAsyncReply producer.Mb (fun reply ->
                        BeginSendMessage(struct(Unchecked.defaultof<MessageBuilder<byte[]>>, reply, false)))
                let! _ = expectFailure<NullReferenceException> sending
                let! _ = expectFailure<NullReferenceException> crashing
                expectTimersStopped producer
            finally
                release.TrySetResult() |> ignore

            let! sent = blockedWrite.WaitAsync(testTimeout)
            Expect.isTrue sent "The shared writer should resume normally"
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "Discard the failed send and close the producer"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = command.CloseProducer.RequestId)))
            Expect.isTrue broker.Connection.IsActive "Failed producer cleanup must preserve the shared connection"
        }

        testTask "Requests after a mailbox failure keep its cause" {
            use broker = new BrokerConnection(clientConfig)
            let! (producer: ProducerImpl<byte[]>) = broker.StartProducer(producerConfig, ProducerInterceptors<byte[]>.Empty, ignore)
            let crashing =
                postAndAsyncReply producer.Mb (fun reply ->
                    BeginSendMessage(struct(Unchecked.defaultof<MessageBuilder<byte[]>>, reply, false)))
            let! _ = expectFailure<NullReferenceException> crashing
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "The failed producer should be closed at the broker"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = command.CloseProducer.RequestId)))

            let failedProducer = producer :> IProducer<byte[]>
            // sends and flushes check the producer state before they post
            let! (sendError: NotConnectedException) =
                expectFailure<NotConnectedException> (task { return! failedProducer.SendAsync([| 1uy |]) })
            Expect.isTrue (sendError.InnerException :? NullReferenceException) "A send should report why the producer failed"
            let! (flushError: NotConnectedException) =
                expectFailure<NotConnectedException> (task { return! failedProducer.FlushAsync() })
            Expect.isTrue (flushError.InnerException :? NullReferenceException) "A flush should report why the producer failed"
            // GetStats still posts, so the stopped mailbox answers it
            let! (statsError: NotConnectedException) = expectFailure<NotConnectedException> (failedProducer.GetStats())
            Expect.isTrue (statsError.InnerException :? NullReferenceException) "The stopped mailbox should report why the producer failed"
        }

        testTask "Producer closes itself when its topic no longer exists" {
            use broker = new BrokerConnection(clientConfig)
            let config = { producerConfig with BatchingEnabled = false; SendTimeout = TimeSpan.Zero }
            let interceptor = new RecordingInterceptor()
            let! (producer: ProducerImpl<byte[]>) = broker.StartProducer(config, ProducerInterceptors<byte[]>([| interceptor |]), ignore)
            let sending = enqueueMessage producer [| 1uy |]
            let! (send: BaseCommand) = broker.ReadCommand()
            Expect.equal send.``type`` BaseCommand.Type.Send "Expected the message to be sent"

            // the topic was deleted, so the broker drops the producer and rejects it when it registers again
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.CloseProducer,
                    CloseProducer = CommandCloseProducer(ProducerId = %(producer :> IProducer<byte[]>).ProducerId, RequestId = 0UL)))
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.Producer "Expected the producer to register again"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Error,
                    Error = CommandError(
                        RequestId = command.Producer.RequestId,
                        Error = ServerError.TopicNotFound,
                        Message = "Topic does not exist")))

            // as in the Java client, it closes instead of reconnecting
            let! (pendingError: AlreadyClosedException) = expectFailure<AlreadyClosedException> sending
            Expect.isTrue (pendingError.InnerException :? TopicDoesNotExistException) "A pending send should report why the producer closed"
            let! _ =
                expectFailure<AlreadyClosedException> (task { return! (producer :> IProducer<byte[]>).SendAsync([| 2uy |]) })
            // the interceptors hear about the failed send before they are disposed, which is the last cleanup step
            do! interceptor.Disposed.WaitAsync(testTimeout)
            Expect.sequenceEqual interceptor.Events [ "failed"; "disposed" ] "Interceptors should be disposed after the pending send fails"
            expectTimersStopped producer
        }

        testTask "Failed close stops all timers and cleans up once" {
            use broker = new BrokerConnection(clientConfig)
            let encryptor =
                { new IMessageEncryptor with
                    member _.Encrypt _ = failwith "No messages should be encrypted"
                    member _.UpdateEncryptionKeys() = () }
            let config = { producerConfig with MessageEncryptor = Some encryptor }
            let mutable cleaned = 0
            let! producer = broker.StartProducer(config, ProducerInterceptors<byte[]>.Empty, fun _ -> cleaned <- cleaned + 1)
            let timers = getTimers producer
            Expect.equal timers.Length 4 "Expected all four producer timers"
            for timer in timers do
                Expect.isTrue timer.Enabled "Timers should be running before close"

            let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "Expected close request"
            broker.Disconnect()
            let! (error: ConnectException) = expectFailure<ConnectException> closing
            Expect.equal error.Message "Disconnected." "Preserve the close failure"
            expectTimersStopped producer
            Expect.equal cleaned 1 "Producer must be removed from its owner's collection"
            do! (producer :> IProducer<byte[]>).DisposeAsync()
            Expect.equal cleaned 1 "Repeated disposal must not repeat cleanup"
            let! _ = expectFailure<AlreadyClosedException> ((producer :> IProducer<byte[]>).GetStats())
            return ()
        }

        testTask "Rejected close fails pending sends, blocked sends and flush, and deregisters the producer" {
            use broker = new BrokerConnection(clientConfig)
            let config =
                { producerConfig with
                    BatchingEnabled = false
                    SendTimeout = TimeSpan.Zero
                    MaxPendingMessages = 1
                    BlockIfQueueFull = true }
            let interceptor = new RecordingInterceptor()
            let! producer = broker.StartProducer(config, ProducerInterceptors<byte[]>([| interceptor |]), ignore)
            let sending = enqueueMessage producer [| 1uy |]
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.Send "First send should reach the connection"
            let blocked = enqueueMessage producer [| 2uy |]
            let flushing = postAndAsyncReply producer.Mb ProducerMessage.Flush
            let! _ = (producer :> IProducer<byte[]>).GetStats().WaitAsync(testTimeout)
            Expect.isFalse sending.IsCompleted "First send should await its receipt"
            Expect.isFalse blocked.IsCompleted "Second send should be blocked by the pending queue"
            Expect.isFalse flushing.IsCompleted "Flush should await the pending send"

            let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
            do! broker.RejectClose()
            let! closeError = expectFailure<BrokerPersistenceException> closing
            for operation in [ sending :> Task; blocked :> Task; flushing :> Task ] do
                let! error = expectFailure<BrokerPersistenceException> operation
                Expect.isTrue (Object.ReferenceEquals(error, closeError)) "Pending work should receive the close failure"
            // as in the Java client, the blocked send hears about its failure too, before the interceptors are disposed
            Expect.sequenceEqual interceptor.Events [ "failed"; "failed"; "disposed" ] "Both sends should reach the interceptors"
            expectTimersStopped producer
            Expect.isTrue broker.Connection.IsActive "A rejected close must not dispose the shared connection"

            // Re-registering the same id succeeds only if the previous producer was removed.
            let registered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            broker.Connection.AddProducer(
                (producer :> IProducer<byte[]>).ProducerId,
                { AckReceived = ignore
                  TopicTerminatedError = ignore
                  RecoverChecksumError = ignore
                  RecoverNotAllowedError = ignore
                  ConnectionClosed = fun _ -> registered.SetResult() })
            broker.Disconnect()
            do! registered.Task.WaitAsync(testTimeout)
        }

        testTask "Timed out close still stops the producer" {
            use broker = new BrokerConnection({ clientConfig with OperationTimeout = TimeSpan.FromSeconds(1.0) })
            let mutable cleaned = false
            let! producer = broker.StartProducer(producerConfig, ProducerInterceptors<byte[]>.Empty, fun _ -> cleaned <- true)
            let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "Leave the close request unanswered"
            let! (error: TimeoutException) = expectFailure<TimeoutException> closing
            Expect.stringContains error.Message "CloseProducer" "The broker request should time out"
            expectTimersStopped producer
            Expect.isTrue cleaned "Timed out close must still clean up the producer"
        }

        for batchBuilder in [ BatchBuilder.Default; BatchBuilder.KeyBased ] do
            testTask $"Rejected close fails buffered messages with {batchBuilder} batching" {
                use broker = new BrokerConnection(clientConfig)
                let config =
                    { producerConfig with
                        BatchBuilder = batchBuilder
                        BatchingMaxPublishDelay = TimeSpan.FromHours(1.0)
                        SendTimeout = TimeSpan.Zero }
                let! producer = broker.StartProducer(config, ProducerInterceptors<byte[]>.Empty, ignore)
                let first = enqueueMessage producer [| 1uy |]
                let second = enqueueMessage producer [| 2uy |]
                let! _ = (producer :> IProducer<byte[]>).GetStats().WaitAsync(testTimeout)
                Expect.isFalse first.IsCompleted "First message should remain buffered"
                Expect.isFalse second.IsCompleted "Second message should remain buffered"
                let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
                do! broker.RejectClose()
                let! _ = expectFailure<BrokerPersistenceException> closing
                let! _ = expectFailure<BrokerPersistenceException> first
                let! _ = expectFailure<BrokerPersistenceException> second
                expectTimersStopped producer
            }

        testTask "Successful close waits for the broker before cleanup" {
            use broker = new BrokerConnection(clientConfig)
            let mutable cleaned = 0
            let! producer = broker.StartProducer(producerConfig, ProducerInterceptors<byte[]>.Empty, fun _ -> cleaned <- cleaned + 1)
            let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.CloseProducer "Expected close request"
            Expect.isFalse closing.IsCompleted "Disposal must wait for the broker response"
            Expect.equal cleaned 0 "Do not clean up before the broker replies"
            Expect.isTrue (getTimers producer |> Array.exists _.Enabled) "Keep timers until the close response"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = command.CloseProducer.RequestId)))
            do! closing.WaitAsync(testTimeout)
            expectTimersStopped producer
            Expect.equal cleaned 1 "Successful close must clean up once"
            Expect.isTrue broker.Connection.IsActive "Successful close must preserve the shared connection"
        }

        testTask "Close fails the batched sends through the interceptors" {
            for batchBuilder in [ BatchBuilder.Default; BatchBuilder.KeyBased ] do
                use broker = new BrokerConnection(clientConfig)
                let config =
                    { producerConfig with
                        BatchingEnabled = true
                        BatchingMaxPublishDelay = TimeSpan.FromHours(1.0)
                        BatchBuilder = batchBuilder
                        SendTimeout = TimeSpan.Zero }
                let interceptor = new RecordingInterceptor()
                let! (producer: ProducerImpl<byte[]>) = broker.StartProducer(config, ProducerInterceptors<byte[]>([| interceptor |]), ignore)
                let batched = enqueueMessage producer [| 1uy |]
                let! _ = (producer :> IProducer<byte[]>).GetStats().WaitAsync(testTimeout)
                Expect.isFalse batched.IsCompleted $"{batchBuilder}: the send should wait in the batch"

                let closing = (producer :> IProducer<byte[]>).DisposeAsync().AsTask()
                let! (command: BaseCommand) = broker.ReadCommand()
                Expect.equal command.``type`` BaseCommand.Type.CloseProducer $"{batchBuilder}: expected close request"
                do! broker.Reply(
                    BaseCommand(
                        ``type`` = BaseCommand.Type.Success,
                        Success = CommandSuccess(RequestId = command.CloseProducer.RequestId)))
                do! closing.WaitAsync(testTimeout)
                let! _ = expectFailure<AlreadyClosedException> batched
                // as in the Java client, discarding the batch fails each of its sends through the interceptors
                Expect.sequenceEqual interceptor.Events [ "failed"; "disposed" ] $"{batchBuilder}: the batched send should reach the interceptors"
        }
    ]
