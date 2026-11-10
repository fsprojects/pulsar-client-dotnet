module Pulsar.Client.UnitTests.Internal.ConsumerImplTests

open System
open System.IO
open System.IO.Pipelines
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

// the consumer sends other commands in between, such as the one that asks for messages
let private readCommandOf (commandType: BaseCommand.Type) (broker: BrokerConnection) =
    task {
        let mutable found: BaseCommand = null
        while isNull found do
            let! (command: BaseCommand) = broker.ReadCommand()
            if command.``type`` = commandType then
                found <- command
        return found
    }

let private readClose (broker: BrokerConnection) =
    readCommandOf BaseCommand.Type.CloseConsumer broker

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

            // the mailbox waits for the broker's answer to a seek, like a mailbox that waits for something that doesn't end
            let seeking: Task<unit> = (consumer :> IConsumer<byte[]>).SeekAsync(MessageId.Earliest)
            let! (seek: BaseCommand) = readCommandOf BaseCommand.Type.Seek broker
            let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()

            // as in the Java client, the close takes no longer than its request can take, and the broker is told
            let! (_: BaseCommand) = readClose broker
            do! closing.WaitAsync(testTimeout)
            Expect.isFalse seeking.IsCompleted "The mailbox still waits for the seek"
            Expect.throwsT<AlreadyClosedException> (fun () -> (consumer :> IConsumer<byte[]>).ReceiveAsync() |> ignore)
                "The consumer is closed"

            // the mailbox finishes the close once it gets to it
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = seek.Seek.RequestId)))
            let! _ = expectFailure<AlreadyClosedException> pendingReceive
            let! _ = expectFailure<AlreadyClosedException> seeking
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

        testTask "A consumer whose mailbox stalls in its close is closed at the broker without it" {
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

            // the mailbox begins to close, and waits for the broker
            let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()
            let! (mailboxClose: BaseCommand) = readClose broker

            // the close is finished without the mailbox, on the connection the mailbox was closing on
            let! (close: BaseCommand) = readClose broker
            Expect.notEqual close.CloseConsumer.RequestId mailboxClose.CloseConsumer.RequestId "The broker is asked again"
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Success,
                    Success = CommandSuccess(RequestId = close.CloseConsumer.RequestId)))
            do! closing.WaitAsync(testTimeout)
        }

        testTask "A consumer closed without its mailbox closes its connection when the broker refuses the close" {
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

            // the mailbox waits for the broker's answer to a seek, and never gets to the close
            let seeking: Task<unit> = (consumer :> IConsumer<byte[]>).SeekAsync(MessageId.Earliest)
            seeking.ContinueWith(fun (seek: Task<unit>) -> seek.Exception |> ignore) |> ignore
            let! (_: BaseCommand) = readCommandOf BaseCommand.Type.Seek broker
            let closing = (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask()
            let! (close: BaseCommand) = readClose broker
            do! broker.Reply(
                BaseCommand(
                    ``type`` = BaseCommand.Type.Error,
                    Error = CommandError(
                        RequestId = close.CloseConsumer.RequestId,
                        Error = ServerError.PersistenceError,
                        Message = "Close rejected")))
            do! closing.WaitAsync(testTimeout)

            // The consumer is off its connection, so the broker only lets go of it with the connection. An exclusive
            // subscription would stay taken otherwise.
            let deadline = DateTime.UtcNow + testTimeout
            while broker.Connection.IsActive && DateTime.UtcNow < deadline do
                do! Task.Delay(10)
            Expect.isFalse broker.Connection.IsActive "The connection is closed"
        }

        testTask "A consumer closed without its mailbox closes its connection when the close can't be sent" {
            // the connection keeps its default timeouts, and its keep-alive doesn't run within the test
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

            // The connection waits for a write that doesn't end, as on a socket the broker has stopped reading. It
            // sends nothing that comes after, and a request only gets its timeout when it is sent.
            let write = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            broker.Connection.SendAndForget(struct ((fun (_: PipeWriter) -> write.Task), BaseCommand.Type.Ping))
            try
                // neither the mailbox's close nor the one made without the mailbox is sent
                do! (consumer :> IConsumer<byte[]>).DisposeAsync().AsTask().WaitAsync(testTimeout)

                // The consumer is off its connection, so the broker only lets go of it with the connection. An
                // exclusive subscription would stay taken otherwise.
                let deadline = DateTime.UtcNow + testTimeout
                while broker.Connection.IsActive && DateTime.UtcNow < deadline do
                    do! Task.Delay(10)
                Expect.isFalse broker.Connection.IsActive "The connection is closed"
            finally
                write.TrySetResult() |> ignore
        }
    ]
