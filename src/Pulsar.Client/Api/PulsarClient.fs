namespace Pulsar.Client.Api

open System
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging
open System.Collections.Generic
open System.Threading.Tasks
open Pulsar.Client.Common
open System.Threading

type PulsarClientState =
    | Active
    | Closing
    | Closed

type PulsarClientMessage =
    | RemoveProducer of IProducer
    | RemoveConsumer of IConsumer
    | AddProducer of IProducer
    | AddConsumer of IConsumer
    | Close of AsyncReplyChannel<Task>
    | Stop

type PulsarClient(config: PulsarClientConfiguration) as this =

    let connectionPool = ConnectionPool(config)
    let lookupService = BinaryLookupService(config, connectionPool)
    let producers = HashSet<IProducer>()
    let consumers = HashSet<IConsumer>()
    let mutable clientState = Active

    let tryStopMailbox() =
        match this.ClientState with
        | Closing ->
            if consumers.Count = 0 && producers.Count = 0 then
                this.Mb.Post(Stop)
        | _ ->
            ()

    let checkIfActive() =
        match this.ClientState with
        | Active ->  ()
        | _ ->  raise <| AlreadyClosedException("Client already closed. State: " + this.ClientState.ToString())

    let mb = MailboxProcessor<PulsarClientMessage>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | RemoveProducer producer ->
                    producers.Remove(producer) |> ignore
                    tryStopMailbox()
                    return! loop ()
                | RemoveConsumer consumer ->
                    consumers.Remove(consumer) |> ignore
                    tryStopMailbox ()
                    return! loop ()
                | AddProducer producer ->
                    producers.Add producer |> ignore
                    return! loop ()
                | AddConsumer consumer ->
                    consumers.Add consumer |> ignore
                    return! loop ()
                | Close channel ->
                    match this.ClientState with
                    | Active ->
                        Log.Logger.LogInformation("Client closing. URL: {0}", config.ServiceAddresses)
                        this.ClientState <- Closing
                        let producersTasks = producers |> Seq.map (fun producer -> producer.CloseAsync())
                        let consumerTasks = consumers |> Seq.map (fun consumer -> consumer.CloseAsync())
                        task {
                            try
                                let! _ = Task.WhenAll (seq { yield! producersTasks; yield! consumerTasks })
                                tryStopMailbox()
                            with ex ->
                                Log.Logger.LogError(ex, "Couldn't stop client")
                                this.ClientState <- Active
                        } |> channel.Reply
                        return! loop ()
                    | _ ->
                        channel.Reply(Task.FromException(AlreadyClosedException("Client already closed. URL: " + config.ServiceAddresses.ToString())))
                        return! loop ()
                | Stop ->
                    this.ClientState <- Closed
                    connectionPool.Close()
                    Log.Logger.LogInformation("Pulsar client stopped")
            }
        loop ()
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "PulsarClient mailbox failure"))

    static member Logger
        with get () = Log.Logger
        and set (value) = Log.Logger <- value

    member this.SubscribeAsync consumerConfig =
        task {
            checkIfActive()
            return! this.SingleTopicSubscribeAsync consumerConfig
        }

    member this.CloseAsync() =
        task {
            checkIfActive()
            let! t = mb.PostAndAsyncReply(Close)
            return! t
        }
        
    member private this.GetPartitionedTopicMetadata(topicName, backoff: Backoff, remainingTimeMs) =
        async {
            try
                return! lookupService.GetPartitionedTopicMetadata topicName |> Async.AwaitTask
            with ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                // skip retry scheduler when set lookup throttle in client or server side which will lead to `TooManyRequestsException`
                let isLookupThrottling = (ex :?> AggregateException).InnerExceptions |> Seq.exists (fun e -> e :? TooManyRequestsException)
                if delay <= 0 || isLookupThrottling then
                    reraize ex
                Log.Logger.LogWarning(ex, "Could not get connection while getPartitionedTopicMetadata -- Will try again in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetPartitionedTopicMetadata(topicName, backoff, remainingTimeMs - delay)
        }
        
    member private this.GetPartitionedTopicMetadata(topicName) =
        task {
            checkIfActive()
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            return! this.GetPartitionedTopicMetadata(topicName, backoff, int config.OperationTimeout.TotalMilliseconds)
        }
    

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration) =
        task {
            checkIfActive()
            Log.Logger.LogDebug("SingleTopicSubscribeAsync started")
            let! metadata = this.GetPartitionedTopicMetadata consumerConfig.Topic.CompleteTopicName
            let removeConsumer = fun consumer -> mb.Post(RemoveConsumer consumer)
            if (metadata.Partitions > 0)
            then
                let! consumer = MultiTopicsConsumerImpl.Init(consumerConfig, config, connectionPool, metadata.Partitions, lookupService, removeConsumer)
                mb.Post(AddConsumer consumer)
                return consumer :> IConsumer
            else
                let! consumer = ConsumerImpl.Init(consumerConfig, config, connectionPool, -1, None, lookupService, true, removeConsumer)
                mb.Post(AddConsumer consumer)
                return consumer :> IConsumer
        }

    member this.CreateProducerAsync (producerConfig: ProducerConfiguration) =
        task {
            checkIfActive()
            Log.Logger.LogDebug("CreateProducerAsync started")
            let! metadata = this.GetPartitionedTopicMetadata producerConfig.Topic.CompleteTopicName
            let removeProducer = fun producer -> mb.Post(RemoveProducer producer)
            if (metadata.Partitions > 0) then
                let! producer = PartitionedProducerImpl.Init(producerConfig, config, connectionPool, metadata.Partitions, lookupService, removeProducer)
                mb.Post(AddProducer producer)
                return producer
            else
                let! producer = ProducerImpl.Init(producerConfig, config, connectionPool, -1, lookupService, removeProducer)
                mb.Post(AddProducer producer)
                return producer
        }

    member this.CreateReaderAsync (readerConfig: ReaderConfiguration) =
        task {
            checkIfActive()
            Log.Logger.LogDebug("CreateReaderAsync started")
            let! metadata = this.GetPartitionedTopicMetadata readerConfig.Topic.CompleteTopicName
            if (metadata.Partitions > 0)
            then
                return failwith "Topic reader cannot be created on a partitioned topic"
            else
                let! reader = Reader.Init(readerConfig, config, connectionPool, lookupService)
                return reader
        }

    member private this.Mb with get(): MailboxProcessor<PulsarClientMessage> = mb

    member private this.ClientState
        with get() = Volatile.Read(&clientState)
        and set(value) = Volatile.Write(&clientState, value)