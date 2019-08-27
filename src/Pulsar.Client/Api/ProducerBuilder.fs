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
                |> throwIfDefault (fun() ->  ArgumentException("Topic name must be set on the producer builder.")))

    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default)

    member this.Topic topic =
        ProducerBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> TopicName })

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

    member __.EnableBatching enableBatching =
        ProducerBuilder(
            client,
            { config with BatchingEnabled = enableBatching })

    member __.BatchingMaxMessages maxMessagesPerBatch =
        ProducerBuilder(
            client,
            { config with
                MaxMessagesPerBatch =
                    maxMessagesPerBatch
                    |> invalidArgIfLessThanZero "BatchingMaxMessages needs to be non negative integer." })

    member __.BatchingMaxPublishDelay batchingMaxPublishDelay =
        ProducerBuilder(
            client,
            { config with MaxBatchingPublishDelay = batchingMaxPublishDelay })

    member __.SendTimeout sendTimeout =
        ProducerBuilder(
            client,
            { config with
                SendTimeout = sendTimeout })

    member __.CompressionType compressionType =
        ProducerBuilder(
            client,
            { config with
                CompressionType = compressionType })

    member this.CreateAsync() =
        config
        |> verify
        |> client.CreateProducerAsync