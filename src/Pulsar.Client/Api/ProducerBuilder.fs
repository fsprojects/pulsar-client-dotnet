namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System

type ProducerBuilder private (client: PulsarClient, config: ProducerConfiguration, producerInterceptors: ProducerInterceptors) =

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

    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default, ProducerInterceptors.Empty)

    member private this.With(newConfig: ProducerConfiguration) =
        ProducerBuilder(client, newConfig, producerInterceptors)

    member private this.With(newInterceptors: ProducerInterceptors) =
        ProducerBuilder(client, config, newInterceptors)

    member this.Topic topic =
        { config with
            Topic = topic
                |> invalidArgIfBlankString "Topic must not be blank."
                |> fun t -> TopicName(t.Trim()) }
        |> this.With

    member this.ProducerName producerName =
        { config with
            ProducerName = producerName |> invalidArgIfBlankString "ProducerName must not be blank." }
        |> this.With

    member this.MaxPendingMessages maxPendingMessages =
        { config with
            MaxPendingMessages =
                maxPendingMessages
                |> invalidArgIfNotGreaterThanZero "MaxPendingMessages needs to be greater than 0." }
        |> this.With

    member this.MaxPendingMessagesAcrossPartitions maxPendingMessagesAcrossPartitions =
        { config with
            MaxPendingMessagesAcrossPartitions =
                maxPendingMessagesAcrossPartitions
                |> invalidArgIfNotGreaterThanZero "MaxPendingMessagesAcrossPartitions needs to be greater than 0." }
        |> this.With

    member this.EnableBatching enableBatching =
        { config with BatchingEnabled = enableBatching }
        |> this.With

    member this.BatchingMaxMessages batchingMaxMessages =
        { config with
            BatchingMaxMessages =
                batchingMaxMessages
                |> invalidArgIfNotGreaterThanZero "BatchingMaxMessages needs to be greater than 0." }
        |> this.With

    member this.BatchingMaxBytes batchingMaxBytes =
        { config with
            BatchingMaxBytes =
                batchingMaxBytes
                |> invalidArgIfNotGreaterThanZero "BatchingMaxBytes needs to be greater than 0." }
        |> this.With

    member this.BatchingMaxPublishDelay batchingMaxPublishDelay =
        { config with
            BatchingMaxPublishDelay =
                batchingMaxPublishDelay
                |> invalidArgIf ((>=) TimeSpan.Zero) "BatchingMaxPublishDelay needs to be greater than 0." }
        |> this.With

    member this.RoundRobinRouterBatchingPartitionSwitchFrequency frequency =
        { config with
            BatchingPartitionSwitchFrequencyByPublishDelay = frequency
            |> invalidArgIf ((>) 1) "Configured value for partition switch frequency must be >= 1" }
        |> this.With

    member this.BatchBuilder batchBuilder =
        { config with BatchBuilder = batchBuilder }
        |> this.With

    member this.SendTimeout sendTimeout =
        { config with
            SendTimeout = sendTimeout }
        |> this.With

    member this.CompressionType compressionType =
        { config with
            CompressionType = compressionType }
        |> this.With

    member this.MessageRoutingMode messageRoutingMode =
        { config with
            MessageRoutingMode = messageRoutingMode }
        |> this.With

    member this.CustomMessageRouter customMessageRouter =
        { config with
            CustomMessageRouter = customMessageRouter
                |> invalidArgIfDefault "CustomMessageRouter can't be null"
                |> Some }
        |> this.With

    member this.HashingScheme hashingScheme =
        { config with
            HashingScheme = hashingScheme }
        |> this.With

    member this.InitialSequenceId initialSequenceId =
        { config with
                InitialSequenceId = initialSequenceId }
        |> this.With

    member this.Intercept ([<ParamArray>] interceptors: IProducerInterceptor array) =
        if interceptors.Length = 0 then this
        else
            ProducerInterceptors(Array.append producerInterceptors.Interceptors interceptors)
            |> this.With

    member this.CreateAsync() =
        config
        |> verify
        |> client.CreateProducerAsync producerInterceptors

    override this.ToString() =
        config.ToString()
