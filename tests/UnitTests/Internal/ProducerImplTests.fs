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
            let! producer = broker.StartProducer(config, ProducerInterceptors<byte[]>.Empty, ignore)
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
    ]
