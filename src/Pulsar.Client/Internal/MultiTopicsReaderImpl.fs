namespace Pulsar.Client.Internal

open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Common
open Pulsar.Client.Api

open System
open Pulsar.Client.Internal
open Pulsar.Client.Schema

type internal MultiTopicsReaderImpl<'T> private (readerConfig: ReaderConfiguration, clientConfig: PulsarClientConfiguration,
                        connectionPool: ConnectionPool, consumerInitInfo: ConsumerInitInfo<'T>,
                         schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
    let subscriptionName =
        if String.IsNullOrEmpty readerConfig.SubscriptionRolePrefix then
            "mt/reader-" + Guid.NewGuid().ToString("N").Substring(22)
        else
            readerConfig.SubscriptionRolePrefix + "-mt/reader-" + Guid.NewGuid().ToString("N").Substring(22)

    let consumerConfig = {
        ConsumerConfiguration<'T>.Default with
            Topics = seq { readerConfig.Topic } |> Seq.cache
            SubscriptionName = %subscriptionName
            SubscriptionType = SubscriptionType.Exclusive
            SubscriptionMode = SubscriptionMode.NonDurable
            ReceiverQueueSize = readerConfig.ReceiverQueueSize
            ReadCompacted = readerConfig.ReadCompacted
            KeySharedPolicy = readerConfig.KeySharedPolicy
            MessageDecryptor = readerConfig.MessageDecryptor
            AutoUpdatePartitions = readerConfig.AutoUpdatePartitions
            AutoUpdatePartitionsInterval = readerConfig.AutoUpdatePartitionsInterval
    }

    let consumer =
        MultiTopicsConsumerImpl<'T>(consumerConfig, clientConfig, connectionPool, MultiConsumerType.Partitioned consumerInitInfo,
                                    readerConfig.StartMessageId, readerConfig.StartMessageFromRollbackDuration,
                                    lookup, ConsumerInterceptors<'T>.Empty, fun _ -> ())

    let castedConsumer = consumer :> IConsumer<'T>

    member private this.InitInternal() =
        backgroundTask {
            return! consumer.InitInternal()
        }

    static member internal Init(config: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                consumerInitInfo: ConsumerInitInfo<'T>,
                                schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
        backgroundTask {
            let reader = MultiTopicsReaderImpl(config, clientConfig, connectionPool, consumerInitInfo, schema, schemaProvider, lookup)
            do! reader.InitInternal()
            return reader :> IReader<'T>
        }

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
