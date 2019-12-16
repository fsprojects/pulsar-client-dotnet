namespace Pulsar.Client.Api

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive
open System
open Pulsar.Client.Internal

type Reader private (config: ReaderConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool, lookup: BinaryLookupService) =
    let subscriptionName =
        if String.IsNullOrEmpty config.SubscriptionRolePrefix then
            "reader-" + Guid.NewGuid().ToString("N").Substring(22)
        else
            config.SubscriptionRolePrefix + "-reader-" + Guid.NewGuid().ToString("N").Substring(22)

    let consumerConfig = {
        ConsumerConfiguration.Default with
            Topic = config.Topic
            SubscriptionName = subscriptionName
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = config.ReceiverQueueSize
            ReadCompacted = config.ReadCompacted
            ResetIncludeHead = config.ResetIncludeHead
            ConsumerName = config.ReaderName }

    let consumer =
        ConsumerImpl(consumerConfig, clientConfig, connectionPool, config.Topic.PartitionIndex, SubscriptionMode.NonDurable,
                     Some config.StartMessageId, lookup, fun _ -> ())

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