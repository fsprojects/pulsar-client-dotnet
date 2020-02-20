namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System

type ProducerBuilder private (client: PulsarClient, config: ProducerConfiguration) =

    let verify(config : ProducerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the producer builder.")
        |> checkValue
            (fun c ->
                c.MessageRoutingMode
                |> invalidArgIf (fun mode -> mode = MessageRoutingMode.CustomPartition && Option.isNone config.CustomMessageRouter) "Valid router should be set with CustomPartition routing mode.")

    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default)

    member this.Topic topic =
        ProducerBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> fun t -> TopicName(t.Trim()) })

    member this.ProducerName producerName =
        ProducerBuilder(
            client,
            { config with
                ProducerName = producerName |> invalidArgIfBlankString "ProducerName must not be blank." })

    member this.MaxPendingMessages maxPendingMessages =
        ProducerBuilder(
            client,
            { config with
                MaxPendingMessages =
                    maxPendingMessages
                    |> invalidArgIfNotGreaterThanZero "MaxPendingMessages needs to be greater than 0." })

    member this.MaxPendingMessagesAcrossPartitions maxPendingMessagesAcrossPartitions =
        ProducerBuilder(
            client,
            { config with
                MaxPendingMessagesAcrossPartitions =
                    maxPendingMessagesAcrossPartitions
                    |> invalidArgIfNotGreaterThanZero "MaxPendingMessagesAcrossPartitions needs to be greater than 0." })

    member this.EnableBatching enableBatching =
        ProducerBuilder(
            client,
            { config with BatchingEnabled = enableBatching })

    member this.BatchingMaxMessages batchingMaxMessages =
        ProducerBuilder(
            client,
            { config with
                BatchingMaxMessages =
                    batchingMaxMessages
                    |> invalidArgIfNotGreaterThanZero "BatchingMaxMessages needs to be greater than 0." })

    member this.BatchingMaxBytes batchingMaxBytes =
        ProducerBuilder(
            client,
            { config with
                BatchingMaxBytes =
                    batchingMaxBytes
                    |> invalidArgIfNotGreaterThanZero "BatchingMaxBytes needs to be greater than 0." })

    member this.BatchingMaxPublishDelay batchingMaxPublishDelay =
        ProducerBuilder(
            client,
            { config with
                BatchingMaxPublishDelay =
                    batchingMaxPublishDelay
                    |> invalidArgIf ((>=) TimeSpan.Zero) "BatchingMaxPublishDelay needs to be greater than 0." })

    member this.RoundRobinRouterBatchingPartitionSwitchFrequency frequency =
        ProducerBuilder(
            client,
            { config with
                BatchingPartitionSwitchFrequencyByPublishDelay = frequency
                |> invalidArgIf ((>) 1) "Configured value for partition switch frequency must be >= 1" })

    member this.BatchBuilder batchBuilder =
        ProducerBuilder(
            client,
            { config with BatchBuilder = batchBuilder })

    member this.SendTimeout sendTimeout =
        ProducerBuilder(
            client,
            { config with
                SendTimeout = sendTimeout })

    member this.CompressionType compressionType =
        ProducerBuilder(
            client,
            { config with
                CompressionType = compressionType })

    member this.MessageRoutingMode messageRoutingMode =
        ProducerBuilder(
            client,
            { config with
                MessageRoutingMode = messageRoutingMode })

    member this.CustomMessageRouter customMessageRouter =
        ProducerBuilder(
            client,
            { config with
                CustomMessageRouter = customMessageRouter
                    |> invalidArgIfDefault "CustomMessageRouter can't be null"
                    |> Some })

    member this.HashingScheme hashingScheme =
        ProducerBuilder(
            client,
            { config with
                HashingScheme = hashingScheme })

    member this.CreateAsync() =
        config
        |> verify
        |> client.CreateProducerAsync

    override this.ToString() =
        config.ToString()