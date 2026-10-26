module Pulsar.Client.UnitTests.Internal.ConsumerImplTests

open System
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
    ]
