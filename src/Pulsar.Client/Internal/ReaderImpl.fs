namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System
open Pulsar.Client.Internal
open Pulsar.Client.Schema

type internal ReaderImpl<'T> private (readerConfig: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                         schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
    let subscriptionName =
        if String.IsNullOrEmpty readerConfig.SubscriptionRolePrefix then
            "reader-" + Guid.NewGuid().ToString("N").Substring(22)
        else
            readerConfig.SubscriptionRolePrefix + "-reader-" + Guid.NewGuid().ToString("N").Substring(22)

    let consumerConfig = {
        ConsumerConfiguration<'T>.Default with
            Topic = readerConfig.Topic
            SubscriptionName = subscriptionName
            SubscriptionType = SubscriptionType.Exclusive
            SubscriptionMode = SubscriptionMode.NonDurable
            ReceiverQueueSize = readerConfig.ReceiverQueueSize
            ReadCompacted = readerConfig.ReadCompacted
            ResetIncludeHead = readerConfig.ResetIncludeHead
            ConsumerName = readerConfig.ReaderName }

    let consumer =
        ConsumerImpl<'T>(consumerConfig, clientConfig, connectionPool, readerConfig.Topic.PartitionIndex,
                     readerConfig.StartMessageId, lookup, readerConfig.StartMessageFromRollbackDuration, true, schema,
                     schemaProvider, ConsumerInterceptors<'T>.Empty, fun _ -> ())

    let castedConsumer = consumer :> IConsumer<'T>

    member private this.InitInternal() =
        task {
            return! consumer.InitInternal()
        }

    static member internal Init(config: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                                schema: ISchema<'T>, schemaProvider: MultiVersionSchemaInfoProvider option, lookup: BinaryLookupService) =
        task {
            let reader = ReaderImpl(config, clientConfig, connectionPool, schema, schemaProvider, lookup)
            do! reader.InitInternal()
            return reader :> IReader<'T>
        }
        
    interface IReader<'T> with    

        member this.ReadNextAsync() =
            task {
                let! message = castedConsumer.ReceiveAsync()
                do! castedConsumer.AcknowledgeAsync(message.MessageId)
                return message
            }

        member this.SeekAsync(msgId: MessageId) =
            castedConsumer.SeekAsync(msgId)

        member this.SeekAsync(timestamp: uint64) =
            castedConsumer.SeekAsync(timestamp)

        member this.HasReachedEndOfTopic with get() =
            castedConsumer.HasReachedEndOfTopic

        member this.HasMessageAvailableAsync() =
            consumer.HasMessageAvailableAsync()

        member this.Topic with get() =
            castedConsumer.Topic

    interface IAsyncDisposable with
        
        member this.DisposeAsync() =
            castedConsumer.DisposeAsync()
