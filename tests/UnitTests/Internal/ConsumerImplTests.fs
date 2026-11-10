module Pulsar.Client.UnitTests.Internal.ConsumerImplTests

open System
open System.IO
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal.ConnectionHandlerTests
open Pulsar.Client.UnitTests.Internal.ProducerImplTests

let private clientConfig =
    { PulsarClientConfiguration.Default with
        StatsInterval = TimeSpan.FromMinutes(1.0)
        KeepAliveInterval = TimeSpan.FromMinutes(1.0) }

let private consumerConfig =
    { ConsumerConfiguration<byte[]>.Default with
        Topics = [| TopicName("public/default/consumer-creation") |]
        SubscriptionName = %"test-subscription" }

let private replyError (broker: BrokerConnection) (command: BaseCommand) error =
    broker.Reply(
        BaseCommand(
            ``type`` = BaseCommand.Type.Error,
            Error = CommandError(RequestId = command.Subscribe.RequestId, Error = error, Message = string error)))

let private closeConsumer (broker: BrokerConnection) (consumer: ConsumerImpl<byte[]>) =
    task {
        let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()
        // the consumer asks for messages first
        let mutable close: BaseCommand = null
        while isNull close do
            let! (command: BaseCommand) = broker.ReadCommand()
            if command.``type`` = BaseCommand.Type.CloseConsumer then
                close <- command
        do! broker.Reply(
            BaseCommand(
                ``type`` = BaseCommand.Type.Success,
                Success = CommandSuccess(RequestId = close.CloseConsumer.RequestId)))
        do! closing.WaitAsync(testTimeout)
    }

/// Holds the consumer's mailbox until the returned source is completed. The mailbox completes a request whose
/// continuation runs on the mailbox's thread and waits there, like a mailbox that waits for something that doesn't end.
let internal holdMailbox (consumer: ConsumerImpl<byte[]>) : Task<TaskCompletionSource> =
    task {
        let held = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        let release = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        let stats = TaskCompletionSource<ConsumerStats>()
        stats.Task.ContinueWith((fun (_: Task<ConsumerStats>) ->
            held.TrySetResult() |> ignore
            release.Task.Wait(testTimeout) |> ignore), TaskContinuationOptions.ExecuteSynchronously) |> ignore
        // Written from the thread pool: the mailbox allows synchronous continuations, so its loop can run on the
        // writer's thread, and that thread is the one held.
        Task.Run(fun () -> consumer.Mb.Writer.TryWrite(ConsumerMessage.GetStats stats) |> ignore) |> ignore
        do! held.Task.WaitAsync(testTimeout)
        return release
    }

// the consumer asks for messages first
let private readClose (broker: BrokerConnection) =
    task {
        let mutable close: BaseCommand = null
        while isNull close do
            let! (command: BaseCommand) = broker.ReadCommand()
            if command.``type`` = BaseCommand.Type.CloseConsumer then
                close <- command
        return close
    }

// its metadata is missing, which the consumer's mailbox doesn't expect
let private malformedMessage () =
    { MessageId = {
        LedgerId = %1L
        EntryId = %1L
        Type = Single
        Partition = -1
        TopicName = %""
        ChunkMessageIds = None
      }
      ConsumerEpoch = Nullable()
      Metadata = Unchecked.defaultof<Metadata>
      Payload = new MemoryStream [||]
      RedeliveryCount = 0
      MessageKey = ""
      IsKeyBase64Encoded = false
      CheckSumValid = true
      Properties = null
      AckSet = null }

[<Tests>]
let tests =
    testList "ConsumerImpl" [
        testTask "Consumer creation retries an error it can retry" {
            use broker = new BrokerConnection(clientConfig)
            let! (starting: Task<ConsumerImpl<byte[]>>) = broker.CreateConsumer(consumerConfig)
            let! (first: BaseCommand) = broker.ReadCommand()
            Expect.equal first.``type`` BaseCommand.Type.Subscribe "Expected subscription"
            do! replyError broker first ServerError.ServiceNotReady

            // as in the Java client, it subscribes again instead of failing
            let! (second: BaseCommand) = broker.ReadCommand()
            Expect.equal second.``type`` BaseCommand.Type.Subscribe "Expected the consumer to subscribe again"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = second.Subscribe.RequestId)))
            let! (consumer: ConsumerImpl<byte[]>) = starting.WaitAsync(testTimeout)
            do! closeConsumer broker consumer
        }

        testTask "Consumer creation fails at once on an error it can't retry" {
            use broker = new BrokerConnection(clientConfig)
            let! (starting: Task<ConsumerImpl<byte[]>>) = broker.CreateConsumer(consumerConfig)
            let! (command: BaseCommand) = broker.ReadCommand()
            Expect.equal command.``type`` BaseCommand.Type.Subscribe "Expected subscription"
            do! replyError broker command ServerError.AuthorizationError
            let! _ = expectFailure<AuthorizationException> starting
            return ()
        }

        testTask "Consumer fails retained requests queued behind close after recovery" {
            use broker = new BrokerConnection(clientConfig)
            let! (starting: Task<ConsumerImpl<byte[]>>) = broker.CreateConsumer(consumerConfig)
            let! (firstSubscribe: BaseCommand) = broker.ReadCommand()
            Expect.equal firstSubscribe.``type`` BaseCommand.Type.Subscribe "Expected initial subscription"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = firstSubscribe.Subscribe.RequestId)))
            let! (consumer: ConsumerImpl<byte[]>) = starting.WaitAsync(testTimeout)
            let receive: Task<Message<byte[]>> = (consumer :> IConsumer<byte[]>).ReceiveAsync()

            let rawMessage =
                { MessageId = {
                    LedgerId = %1L
                    EntryId = %1L
                    Type = Single
                    Partition = -1
                    TopicName = %""
                    ChunkMessageIds = None
                  }
                  ConsumerEpoch = Nullable()
                  Metadata = Unchecked.defaultof<Metadata>
                  Payload = new MemoryStream [||]
                  RedeliveryCount = 0
                  MessageKey = ""
                  IsKeyBase64Encoded = false
                  CheckSumValid = true
                  Properties = null
                  AckSet = null }
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.MessageReceived(struct(rawMessage, broker.Connection))))
                "The malformed message should reach the consumer mailbox"

            let mutable close: BaseCommand = null
            while isNull close do
                let! (command: BaseCommand) = broker.ReadCommand()
                if command.``type`` = BaseCommand.Type.CloseConsumer then
                    close <- command
            Expect.equal close.``type`` BaseCommand.Type.CloseConsumer "Unexpected mailbox failures should close the old broker consumer"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = close.CloseConsumer.RequestId)))

            let! (reSubscribe: BaseCommand) = broker.ReadCommand()
            Expect.equal reSubscribe.``type`` BaseCommand.Type.Subscribe "The consumer should resubscribe after recovery"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = reSubscribe.Subscribe.RequestId)))

            let metadata =
                { NumMessages = 1
                  NumChunks = 0
                  TotalChunkMsgSize = 0
                  HasNumMessagesInBatch = false
                  CompressionType = CompressionType.None
                  UncompressedMessageSize = 1
                  SchemaVersion = None
                  SequenceId = %0L
                  ChunkId = %0
                  PublishTime = %DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                  Uuid = %""
                  EncryptionKeys = [||]
                  EncryptionParam = [||]
                  EncryptionAlgo = ""
                  EventTime = Nullable()
                  OrderingKey = [||]
                  ReplicatedFrom = ""
                  ProducerName = ""
                  NullValue = false }
            let deliveredMessage =
                { rawMessage with
                    MessageId = { rawMessage.MessageId with EntryId = %2L }
                    Metadata = metadata
                    Payload = new MemoryStream [| 42uy |] }
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.MessageReceived(struct(deliveredMessage, broker.Connection))))
                "The next message should reach the recovered consumer"
            let! (message: Message<byte[]>) = receive.WaitAsync(testTimeout)
            Expect.equal message.Data [| 42uy |] "The pending receive should complete after recovery"

            let malformedMessage = { rawMessage with Payload = new MemoryStream [||] }
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.MessageReceived(struct(malformedMessage, broker.Connection))))
                "A second malformed message should trigger recovery"

            let mutable recoveryClose: BaseCommand = null
            while isNull recoveryClose do
                let! (command: BaseCommand) = broker.ReadCommand()
                if command.``type`` = BaseCommand.Type.CloseConsumer then
                    recoveryClose <- command

            let closeChannel = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let statsChannel = TaskCompletionSource<ConsumerStats>(TaskCreationOptions.RunContinuationsAsynchronously)
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.Close closeChannel))
                "The close request should be queued behind the recovery"
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.GetStats statsChannel))
                "The stats request should be retained after the queued close"

            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = recoveryClose.CloseConsumer.RequestId)))
            do! closeChannel.Task.WaitAsync(testTimeout)
            let! _ = expectFailure<AlreadyClosedException> statsChannel.Task
            return ()
        }

        testTask "Closing completes although the mailbox doesn't get to it" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromMilliseconds(250.0) }
            use broker = new BrokerConnection(clientConfig)
            let! (starting: Task<ConsumerImpl<byte[]>>) = broker.CreateConsumer(consumerConfig)
            let! (subscribe: BaseCommand) = broker.ReadCommand()
            Expect.equal subscribe.``type`` BaseCommand.Type.Subscribe "Expected subscription"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = subscribe.Subscribe.RequestId)))
            let! (consumer: ConsumerImpl<byte[]>) = starting.WaitAsync(testTimeout)
            let pendingReceive: Task<Message<byte[]>> = (consumer :> IConsumer<byte[]>).ReceiveAsync()

            let! (release: TaskCompletionSource) = holdMailbox consumer
            try
                let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()

                // as in the Java client, the close takes no longer than its request can take, and the broker is told
                let! (_: BaseCommand) = readClose broker
                do! closing.WaitAsync(testTimeout)
                Expect.isFalse release.Task.IsCompleted "The mailbox is still held"
                Expect.throwsT<AlreadyClosedException> (fun () -> (consumer :> IConsumer<byte[]>).ReceiveAsync() |> ignore)
                    "The consumer is closed"
            finally
                release.TrySetResult() |> ignore

            // the mailbox finishes the close once it gets to it
            let! _ = expectFailure<AlreadyClosedException> pendingReceive
            return ()
        }

        testTask "A consumer closed while it recovers fails its pending receives" {
            // the connection keeps its default timeouts, so no request timeout ends the broker's silence
            use broker = new BrokerConnection(clientConfig)
            let consumerClientConfig = { clientConfig with OperationTimeout = TimeSpan.FromMilliseconds(250.0) }
            let! (starting: Task<ConsumerImpl<byte[]>>) = broker.CreateConsumer(consumerConfig, consumerClientConfig)
            let! (subscribe: BaseCommand) = broker.ReadCommand()
            Expect.equal subscribe.``type`` BaseCommand.Type.Subscribe "Expected subscription"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = subscribe.Subscribe.RequestId)))
            let! (consumer: ConsumerImpl<byte[]>) = starting.WaitAsync(testTimeout)
            let pendingReceive: Task<Message<byte[]>> = (consumer :> IConsumer<byte[]>).ReceiveAsync()

            // the consumer recovers from a message its mailbox can't process, and first closes at the broker
            Expect.isTrue (consumer.Mb.Writer.TryWrite(ConsumerMessage.MessageReceived(struct(malformedMessage (), broker.Connection))))
                "The malformed message should reach the consumer mailbox"
            let! (recoveryClose: BaseCommand) = readClose broker

            // the broker doesn't answer, so the recovery outlasts the close, which then happens without the mailbox
            let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()
            let! (_: BaseCommand) = readClose broker
            do! closing.WaitAsync(testTimeout)
            Expect.isFalse pendingReceive.IsCompleted "Only the mailbox can fail the pending receives"

            // the recovery finds the consumer closed: nothing restarts its mailbox, so the receives are failed here
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = recoveryClose.CloseConsumer.RequestId)))
            let! _ = expectFailure<AlreadyClosedException> pendingReceive
            return ()
        }

        testSequenced <| testTask "A consumer fails its receives when its connection handler stops" {
            use broker = new BrokerConnection(clientConfig)
            let! (starting: Task<ConsumerImpl<byte[]>>) =
                broker.CreateConsumer({ consumerConfig with ConsumerName = "handler-failure-consumer" })
            let! (subscribe: BaseCommand) = broker.ReadCommand()
            Expect.equal subscribe.``type`` BaseCommand.Type.Subscribe "Expected subscription"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = subscribe.Subscribe.RequestId)))
            let! (consumer: ConsumerImpl<byte[]>) = starting.WaitAsync(testTimeout)
            let pendingReceive: Task<Message<byte[]>> = (consumer :> IConsumer<byte[]>).ReceiveAsync()

            let original = Log.Logger
            try
                // the consumer's connection handler fails wherever it deals with the closed connection
                Log.Logger <-
                    FailingLogger(original, fun message ->
                        message.Contains "handler-failure-consumer" && message.Contains "ConnectionHandler")
                broker.Disconnect()

                // nothing connects the consumer again, so its receives must not wait for messages
                let! _ = expectFailure<AlreadyClosedException> pendingReceive
                Expect.throwsT<NotConnectedException> (fun () -> (consumer :> IConsumer<byte[]>).ReceiveAsync() |> ignore)
                    "Later receives are refused"
            finally
                Log.Logger <- original
        }
    ]
