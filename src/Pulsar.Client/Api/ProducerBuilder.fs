namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Threading.Tasks

type ProducerBuilder<'T> private (сreateProducerAsync, config: ProducerConfiguration, producerInterceptors: ProducerInterceptors<'T>, schema: ISchema<'T>) =

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

    internal new(сreateProducerAsync, schema: ISchema<'T>) = ProducerBuilder(сreateProducerAsync, ProducerConfiguration.Default, ProducerInterceptors.Empty, schema)

    member private this.With(newConfig: ProducerConfiguration) =
        ProducerBuilder<'T>(сreateProducerAsync, newConfig, producerInterceptors, schema)

    member private this.With(newInterceptors: ProducerInterceptors<'T>) =
        ProducerBuilder<'T>(сreateProducerAsync, config, newInterceptors, schema)

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

    member this.Intercept ([<ParamArray>] interceptors: IProducerInterceptor<'T> array) =
        if interceptors.Length = 0 then this
        else
            ProducerInterceptors(Array.append producerInterceptors.Interceptors interceptors)
            |> this.With
    
    member this.CreateAsync(): Task<IProducer<'T>> =
        сreateProducerAsync(verify config, schema, producerInterceptors)

    override this.ToString() =
        config.ToString()
