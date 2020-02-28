namespace Pulsar.Client.Api

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System
open Pulsar.Client.Internal

type Reader private (readerConfig: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool, lookup: BinaryLookupService) =
    let subscriptionName =
        if String.IsNullOrEmpty readerConfig.SubscriptionRolePrefix then
            "reader-" + Guid.NewGuid().ToString("N").Substring(22)
        else
            readerConfig.SubscriptionRolePrefix + "-reader-" + Guid.NewGuid().ToString("N").Substring(22)

    let consumerConfig = {
        ConsumerConfiguration.Default with
            Topic = readerConfig.Topic
            SubscriptionName = subscriptionName
            SubscriptionType = SubscriptionType.Exclusive
            SubscriptionMode = SubscriptionMode.NonDurable
            ReceiverQueueSize = readerConfig.ReceiverQueueSize
            ReadCompacted = readerConfig.ReadCompacted
            ResetIncludeHead = readerConfig.ResetIncludeHead
            ConsumerName = readerConfig.ReaderName }

    let consumer =
        ConsumerImpl(consumerConfig, clientConfig, connectionPool, readerConfig.Topic.PartitionIndex,
                     readerConfig.StartMessageId, lookup, readerConfig.StartMessageFromRollbackDuration, true, fun _ -> ())

    let castedConsumer = consumer :> IConsumer

    member private this.InitInternal() =
        task {
            return! consumer.InitInternal()
        }

    static member internal Init(config: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool, lookup: BinaryLookupService) =
        task {
            let reader = Reader(config, clientConfig, connectionPool, lookup)
            do! reader.InitInternal()
            return reader
        }

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

    member this.CloseAsync() =
        castedConsumer.CloseAsync()