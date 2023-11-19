namespace Pulsar.Client.Internal

open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Common
open Pulsar.Client.Api

open System
open Pulsar.Client.Internal
open Pulsar.Client.Schema

type internal ReaderImpl<'T> private (readerConfig: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                         schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
    let subscriptionName =
        if String.IsNullOrEmpty readerConfig.SubscriptionRolePrefix |> not then
            readerConfig.SubscriptionRolePrefix + "-reader-" + Guid.NewGuid().ToString("N").Substring(22)
        elif String.IsNullOrEmpty readerConfig.SubscriptionName |> not then
            readerConfig.SubscriptionName
        else
            "reader-" + Guid.NewGuid().ToString("N").Substring(22)

    let consumerConfig = {
        ConsumerConfiguration<'T>.Default with
            Topics = seq { readerConfig.Topic } |> Seq.cache
            SubscriptionName = %subscriptionName
            SubscriptionType = SubscriptionType.Exclusive
            SubscriptionMode = SubscriptionMode.NonDurable
            ReceiverQueueSize = readerConfig.ReceiverQueueSize
            ReadCompacted = readerConfig.ReadCompacted
            ResetIncludeHead = readerConfig.ResetIncludeHead
            ConsumerName = readerConfig.ReaderName
            KeySharedPolicy = readerConfig.KeySharedPolicy
            MessageDecryptor = readerConfig.MessageDecryptor
            // Reader doesn't need any batch receiving behaviours
            // disable the batch receive timer for the ConsumerImpl instance wrapped by the ReaderImpl
            BatchReceivePolicy = ReaderImpl<'T>.DISABLED_BATCH_RECEIVE_POLICY
    }

    let consumer =
        ConsumerImpl<'T>(consumerConfig, clientConfig, readerConfig.Topic, connectionPool, readerConfig.Topic.PartitionIndex,
                     false, readerConfig.StartMessageId, readerConfig.StartMessageFromRollbackDuration, lookup, true, schema,
                     schemaProvider, ConsumerInterceptors<'T>.Empty, fun _ -> ())

    let castedConsumer = consumer :> IConsumer<'T>

    member private this.InitInternal() =
        backgroundTask {
            return! consumer.InitInternal()
        }

    static member internal Init(config: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
        backgroundTask {
            let reader = ReaderImpl(config, clientConfig, connectionPool, schema, schemaProvider, lookup)
            do! reader.InitInternal()
            return reader :> IReader<'T>
        }

    static member internal DISABLED_BATCH_RECEIVE_POLICY =
            BatchReceivePolicy(1, 0L, TimeSpan.Zero)

    interface IReader<'T> with

        member this.ReadNextAsync() =
            backgroundTask {
                let! message = castedConsumer.ReceiveAsync()
                castedConsumer.AcknowledgeCumulativeAsync(message.MessageId) |> ignore
                return message
            }

        member this.ReadNextAsync ct =
            backgroundTask {
                let! message = castedConsumer.ReceiveAsync(ct)
                castedConsumer.AcknowledgeCumulativeAsync(message.MessageId) |> ignore
                return message
            }

        member this.SeekAsync(msgId: MessageId) =
            castedConsumer.SeekAsync(msgId)

        member this.SeekAsync(timestamp: TimeStamp) =
            castedConsumer.SeekAsync(timestamp)

        member this.SeekAsync (resolver: Func<string, SeekType>) : Task<Unit>  =
            castedConsumer.SeekAsync(resolver)

        member this.HasReachedEndOfTopic with get() =
            castedConsumer.HasReachedEndOfTopic

        member this.HasMessageAvailableAsync() =
            consumer.HasMessageAvailableAsync()

        member this.Topic with get() =
            castedConsumer.Topic

        member this.IsConnected with get() =
            castedConsumer.IsConnected

    interface IAsyncDisposable with

        member this.DisposeAsync() =
            castedConsumer.DisposeAsync()
