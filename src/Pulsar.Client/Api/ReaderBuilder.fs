namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common

type ReaderBuilder private (client: PulsarClient, config: ReaderConfiguration) =

    let verify(config : ReaderConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the reader builder.")
        |> checkValue
            (fun c ->
                if c.StartMessageId.IsSome && c.StartMessageFromRollbackDuration > TimeSpan.Zero
                    || c.StartMessageId.IsNone && c.StartMessageFromRollbackDuration = TimeSpan.Zero then                
                    failwith "Start message id or start message from roll back must be specified but they cannot be specified at the same time"
                elif c.StartMessageFromRollbackDuration > TimeSpan.Zero then
                    { config with
                        StartMessageId = Some MessageId.Earliest }
                else
                    c)

    new(client: PulsarClient) = ReaderBuilder(client, ReaderConfiguration.Default)

    member this.Topic topic =
        ReaderBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> fun t -> TopicName(t.Trim()) })

    member this.StartMessageId messageId =
        ReaderBuilder(
            client,
            { config with
                StartMessageId = messageId
                    |> invalidArgIfDefault "MessageId can't be null"
                    |> Some })

    member this.StartMessageIdInclusive (startMessageIdInclusive: bool) =
        ReaderBuilder(
            client,
            { config with
                ResetIncludeHead = startMessageIdInclusive })

    member this.ReadCompacted readCompacted =
        ReaderBuilder(
            client,
            { config with
                ReadCompacted = readCompacted })

    member this.SubscriptionRolePrefix subscriptionRolePrefix =
        ReaderBuilder(
            client,
            { config with
                SubscriptionRolePrefix = subscriptionRolePrefix })

    member this.ReaderName readerName =
        ReaderBuilder(
            client,
            { config with
                ReaderName = readerName |> invalidArgIfBlankString "ReaderName must not be blank." })

    member this.ReceiverQueueSize receiverQueueSize =
        ReaderBuilder(
            client,
            { config with
                ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  })
                
    member this.StartMessageFromRollbackDuration rollbackDuration =
        ReaderBuilder(
            client,
            { config with
                StartMessageFromRollbackDuration = rollbackDuration })

    member this.CreateAsync() =
        config
        |> verify
        |> client.CreateReaderAsync

