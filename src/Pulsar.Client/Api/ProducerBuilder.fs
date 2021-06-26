namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Threading.Tasks

type ProducerBuilder<'T> private (сreateProducerAsync, config: ProducerConfiguration, producerInterceptors: ProducerInterceptors<'T>, schema: ISchema<'T>) =

    let verify(config : ProducerConfiguration) =
        
        config
        |> (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the producer builder."
                |> fun _ -> c
            )
        |> invalidArgIf (fun c ->
                c.BatchingEnabled && c.ChunkingEnabled
            ) "Batching and chunking of messages can't be enabled together"
        |> invalidArgIf (fun c ->
                c.MessageRoutingMode = MessageRoutingMode.CustomPartition && Option.isNone config.CustomMessageRouter
            ) "Valid router should be set with CustomPartition routing mode."
        |> invalidArgIf (fun c ->
                c.MaxPendingMessagesAcrossPartitions < c.MaxPendingMessages
            ) "MaxPendingMessagesAcrossPartitions needs to be >= MaxPendingMessages."

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
                |> invalidArgIfLessThanZero "MaxPendingMessages needs to be >= 0." }
        |> this.With

    member this.MaxPendingMessagesAcrossPartitions maxPendingMessagesAcrossPartitions =
        { config with
            MaxPendingMessagesAcrossPartitions =
                maxPendingMessagesAcrossPartitions
                |> invalidArgIfLessThanZero "MaxPendingMessagesAcrossPartitions needs to be >= 0." }
        |> this.With

    member this.EnableBatching enableBatching =
        { config with BatchingEnabled = enableBatching }
        |> this.With

    member this.EnableChunking enableChunking =
        { config with ChunkingEnabled = enableChunking }
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
            InitialSequenceId = Some initialSequenceId }
        |> this.With

    member this.Intercept ([<ParamArray>] interceptors: IProducerInterceptor<'T> array) =
        if interceptors.Length = 0 then this
        else
            ProducerInterceptors(Array.append producerInterceptors.Interceptors interceptors)
            |> this.With
            
    member this.BlockIfQueueFull blockIfQueueFull =
        { config with
            BlockIfQueueFull = blockIfQueueFull }
        |> this.With
        
    member this.AutoUpdatePartitions autoUpdatePartitions =
        { config with
            AutoUpdatePartitions = autoUpdatePartitions }
        |> this.With
        
    member this.AutoUpdateInterval autoUpdatePartitionsInterval =
        { config with
            AutoUpdatePartitionsInterval = autoUpdatePartitionsInterval }
        |> this.With
    
    member this.MessageEncryptor messageEncryptor  =
        { config with
            MessageEncryptor = Some messageEncryptor }
        |> this.With
    
    member this.CreateAsync(): Task<IProducer<'T>> =
        сreateProducerAsync(verify config, schema, producerInterceptors)

    member this.Configuration =
        config
