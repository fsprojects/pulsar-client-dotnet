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

        testTask "Consumer recovers after an unexpected mailbox exception" {
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
            do! closeConsumer broker consumer
        }
    ]
