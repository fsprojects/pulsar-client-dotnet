namespace Pulsar.Client.Api

open System
open System.Text.RegularExpressions

open Pulsar.Client.Api
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging
open System.Collections.Generic
open System.Threading.Tasks
open Pulsar.Client.Common
open System.Threading
open Pulsar.Client.Schema
open Pulsar.Client.Transaction
open System.Threading.Channels

type internal PulsarClientState =
    | Active
    | Closing
    | Closed

type internal PulsarClientMessage =
    | RemoveProducer of IAsyncDisposable // IProducer
    | RemoveConsumer of IAsyncDisposable // IConsumer
    | AddProducer of IAsyncDisposable // IProducer
    | AddConsumer of IAsyncDisposable // IConsumer
    | GetSchemaProvider of TaskCompletionSource<MultiVersionSchemaInfoProvider> * CompleteTopicName
    | Close of TaskCompletionSource<Unit>
    | Stop

type PulsarClient internal (config: PulsarClientConfiguration) as this =

    let connectionPool = ConnectionPool(config)
    let lookupService = BinaryLookupService(config, connectionPool)
    let producers = HashSet<IAsyncDisposable>()
    let consumers = HashSet<IAsyncDisposable>()
    let schemaProviders = Dictionary<CompleteTopicName, MultiVersionSchemaInfoProvider>()
    let mutable clientState = Active
    let autoProduceStubType =  typeof<AutoProduceBytesSchemaStub>
    let autoConsumeStubType =  typeof<AutoConsumeSchemaStub>
    let transactionClient =
        if config.EnableTransaction then
            TransactionCoordinatorClient(config, connectionPool, lookupService) |> Some
        else
            None

    let tryStopMailbox() =
        match this.ClientState with
        | Closing ->
            if consumers.Count = 0 && producers.Count = 0 then
                post this.Mb Stop
        | _ ->
            ()

    let checkIfActive() =
        match this.ClientState with
        | Active ->  ()
        | _ ->  raise <| AlreadyClosedException("Client already closed. State: " + this.ClientState.ToString())

    let getActiveScmema (schema: ISchema<'T>) (topic:TopicName) =
        backgroundTask {
            let mutable activeSchema = schema
            if schema.GetType() = autoConsumeStubType then
                match! lookupService.GetSchema(topic.CompleteTopicName) with
                | Some schemaData ->
                    let autoSchema = Schema.GetAutoConsumeSchema schemaData |> box
                    activeSchema <- autoSchema |> unbox
                | None ->
                    ()
            return activeSchema
        }

    let mb = Channel.CreateUnbounded<PulsarClientMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! mb.Reader.ReadAsync() with
            | RemoveProducer producer ->
                producers.Remove(producer) |> ignore
                tryStopMailbox()
            | RemoveConsumer consumer ->
                consumers.Remove(consumer) |> ignore
                tryStopMailbox ()
            | AddProducer producer ->
                producers.Add producer |> ignore
            | AddConsumer consumer ->
                consumers.Add consumer |> ignore
            | GetSchemaProvider (channel, topicName) ->
                match schemaProviders.TryGetValue(topicName) with
                | true, provider -> channel.SetResult(provider)
                | false, _ ->
                    let provider =
                        MultiVersionSchemaInfoProvider(fun schemaVersion ->
                            lookupService.GetSchema(topicName, schemaVersion))
                    schemaProviders.Add(topicName, provider)
                    channel.SetResult(provider)
            | Close channel ->
                match this.ClientState with
                | Active ->
                    Log.Logger.LogInformation("Client closing. URL: {0}", config.ServiceAddresses)
                    this.ClientState <- Closing
                    let producersTasks = producers |> Seq.map (fun producer -> backgroundTask { return! producer.DisposeAsync() } )
                    let consumerTasks = consumers |> Seq.map (fun consumer -> backgroundTask { return! consumer.DisposeAsync() })
                    backgroundTask {
                        try
                            let! _ = Task.WhenAll (seq { yield! producersTasks; yield! consumerTasks })
                            schemaProviders |> Seq.iter (fun (KeyValue (_, provider)) -> provider.Close())
                            config.Authentication.Dispose()
                            tryStopMailbox()
                            channel.SetResult()
                        with ex ->
                            Log.Logger.LogError(ex, "Couldn't stop client")
                            this.ClientState <- Active
                            channel.SetResult()
                    } |> ignore
                | _ ->
                    channel.SetException(AlreadyClosedException("Client already closed. URL: " + config.ServiceAddresses.ToString()))
            | Stop ->
                this.ClientState <- Closed
                do! connectionPool.CloseAsync()
                transactionClient |> Option.iter (fun tc -> tc.Close())
                Log.Logger.LogInformation("Pulsar client stopped")
                continueLoop <- false
        } :> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "PulsarClient mailbox failure")
            else
                Log.Logger.LogInformation("PulsarClient mailbox has stopped normally"))
        |> ignore

    let removeConsumer = fun consumer -> post mb (RemoveConsumer consumer)
    let addConsumer = fun consumer -> post mb (AddConsumer consumer)

    static member Logger
        with get () = Log.Logger
        and set value = Log.Logger <- value

    member internal this.Init() =
        backgroundTask {
            match transactionClient with
            | Some tc ->
                match! tc.Start() with
                | Ok () -> ()
                | Error exn -> reraize exn
            | None ->
                ()
        }

    member internal this.SubscribeAsync(consumerConfig : ConsumerConfiguration<'T>, schema, interceptors) =
        checkIfActive()
        if (consumerConfig.TopicsPattern |> String.IsNullOrEmpty |> not) then
            this.PatternTopicSubscribeAsync(consumerConfig, schema, interceptors)
        elif (consumerConfig.Topics |> Seq.length |> (<) 1) then
            this.MultiTopicSubscribeAsync(consumerConfig, schema, interceptors)
        else
            this.SingleTopicSubscribeAsync(consumerConfig, schema, interceptors)

    member this.CloseAsync() =
        checkIfActive()
        postAndAsyncReply mb Close

    member private this.PreProcessSchemaBeforeSubscribe(schema: ISchema<'T>, topicName) =
        backgroundTask {
            if schema.SupportSchemaVersioning then
                let! provider = postAndAsyncReply mb (fun channel -> GetSchemaProvider(channel, topicName))
                return Some provider
            else
                return None
        }

    member private this.GetConsumerInitInfo (schema, topic: TopicName) =
        backgroundTask {
            let! schemaProvider = this.PreProcessSchemaBeforeSubscribe(schema, topic.CompleteTopicName)
            let! metadata = lookupService.GetPartitionedTopicMetadata topic.CompleteTopicName
            let! activeSchema = getActiveScmema schema topic
            return {
                TopicName = topic
                Schema = activeSchema
                SchemaProvider = schemaProvider
                Metadata = metadata
            }
        }

    member private this.GetTopicsByPattern (fakeTopicName: TopicName) (regex: Regex) =
        fun () ->
            backgroundTask {
                let! allNamespaceTopics = lookupService.GetTopicsUnderNamespace(fakeTopicName.NamespaceName, fakeTopicName.IsPersistent)
                let topics =
                    allNamespaceTopics
                    |> Seq.filter regex.IsMatch
                    |> Seq.map TopicName
                    |> Seq.toArray
                return topics
            }

    member private this.PatternTopicSubscribeAsync (consumerConfig: ConsumerConfiguration<'T>, schema: ISchema<'T>, interceptors: ConsumerInterceptors<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("PatternTopicSubscribeAsync started")
            let fakeTopicName = TopicName(consumerConfig.TopicsPattern)
            let regex = Regex(consumerConfig.TopicsPattern)
            let getTopicsFun = this.GetTopicsByPattern fakeTopicName regex
            let getConsumerInfoFun = fun topic -> this.GetConsumerInitInfo(schema, topic)
            let! topics = getTopicsFun()
            let! consumerInfos =
                if topics.Length > 0 then
                    topics
                    |> Seq.map (fun topic -> this.GetConsumerInitInfo(schema, topic))
                    |> Task.WhenAll
                else
                    Task.FromResult([||])
            let patternInfo = { InitialTopics = consumerInfos; GetTopics = getTopicsFun; GetConsumerInfo = getConsumerInfoFun }
            let! consumer = MultiTopicsConsumerImpl.InitPattern(consumerConfig, config, connectionPool,
                                                            patternInfo, lookupService, interceptors, removeConsumer)
            addConsumer consumer
            return consumer :> IConsumer<'T>
        }

    member private this.MultiTopicSubscribeAsync (consumerConfig: ConsumerConfiguration<'T>, schema: ISchema<'T>, interceptors: ConsumerInterceptors<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("MultiTopicSubscribeAsync started")
            let! partitionsForTopis =
                consumerConfig.Topics
                |> Seq.map (fun topic -> this.GetConsumerInitInfo(schema, topic))
                |> Task.WhenAll
            let! consumer = MultiTopicsConsumerImpl.InitMultiTopic(consumerConfig, config, connectionPool, partitionsForTopis,
                                                             lookupService, interceptors, removeConsumer)
            addConsumer consumer
            return consumer :> IConsumer<'T>
        }

    member private this.SingleTopicSubscribeAsync (consumerConfig: ConsumerConfiguration<'T>, schema: ISchema<'T>, interceptors: ConsumerInterceptors<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("SingleTopicSubscribeAsync started")
            let topic = consumerConfig.SingleTopic
            let! schemaProvider = this.PreProcessSchemaBeforeSubscribe(schema, topic.CompleteTopicName)
            let! metadata = lookupService.GetPartitionedTopicMetadata topic.CompleteTopicName
            let! activeSchema = getActiveScmema schema topic
            if metadata.IsMultiPartitioned then
                let consumerInitInfo = {
                    TopicName = topic
                    Schema = activeSchema
                    SchemaProvider = schemaProvider
                    Metadata = metadata
                }
                let! consumer = MultiTopicsConsumerImpl.InitPartitioned(consumerConfig, config, connectionPool, consumerInitInfo,
                                                             lookupService, interceptors, removeConsumer)
                addConsumer consumer
                return consumer :> IConsumer<'T>
            else
                let! consumer = ConsumerImpl.Init(consumerConfig, config, consumerConfig.SingleTopic, connectionPool, -1, false,
                                                  None, TimeSpan.Zero, lookupService, true, activeSchema, schemaProvider,
                                                  interceptors, removeConsumer)
                addConsumer consumer
                return consumer :> IConsumer<'T>
        }

    member internal this.CreateProducerAsync<'T> (producerConfig: ProducerConfiguration, schema: ISchema<'T>, interceptors: ProducerInterceptors<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("CreateProducerAsync started")
            let! metadata = lookupService.GetPartitionedTopicMetadata producerConfig.Topic.CompleteTopicName
            let mutable activeSchema = schema
            if schema.GetType() = autoProduceStubType then
                match! lookupService.GetSchema(producerConfig.Topic.CompleteTopicName) with
                | Some schemaInfo ->
                    let validate = Schema.GetValidateFunction schemaInfo
                    let autoProduceSchema = AutoProduceBytesSchema(schemaInfo.SchemaInfo.Name, schemaInfo.SchemaInfo.Type, schemaInfo.SchemaInfo.Schema, validate) |> box
                    activeSchema <- autoProduceSchema |> unbox
                | None ->
                    ()
            let removeProducer = fun producer -> post mb (RemoveProducer producer)
            if metadata.IsMultiPartitioned then
                let! producer = PartitionedProducerImpl.Init(producerConfig, config, connectionPool, metadata.Partitions,
                                                             lookupService, activeSchema, interceptors, removeProducer)
                post mb (AddProducer producer)
                return producer :> IProducer<'T>
            else
                let! producer = ProducerImpl.Init(producerConfig, config, connectionPool, -1, lookupService,
                                                  activeSchema, interceptors, removeProducer)
                post mb (AddProducer producer)
                return producer :> IProducer<'T>
        }

    member internal this.CreateReaderAsync<'T> (readerConfig: ReaderConfiguration, schema: ISchema<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("CreateReaderAsync started")
            let! metadata = lookupService.GetPartitionedTopicMetadata readerConfig.Topic.CompleteTopicName
            let! schemaProvider = this.PreProcessSchemaBeforeSubscribe(schema, readerConfig.Topic.CompleteTopicName)
            let! activeSchema = getActiveScmema schema readerConfig.Topic
            let! reader =
                if metadata.IsMultiPartitioned then
                    if MultiTopicsConsumerImpl<_>.isIllegalMultiTopicsMessageId readerConfig.StartMessageId.Value then
                        failwith "The partitioned topic startMessageId is illegal"
                    let consumerInitInfo = {
                        TopicName = readerConfig.Topic
                        Schema = activeSchema
                        SchemaProvider = schemaProvider
                        Metadata = metadata
                    }
                    MultiTopicsReaderImpl.Init(readerConfig, config, connectionPool, consumerInitInfo,
                                                             schema, schemaProvider, lookupService)
                else
                    ReaderImpl.Init(readerConfig, config, connectionPool, schema, schemaProvider, lookupService)
            post mb (AddConsumer reader)
            return reader
        }

    member internal this.CreateTableViewReaderAsync<'T> (tableViewConfig: TableViewConfiguration, schema: ISchema<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("CreateTableViewReaderAsync started")
            let! metadata = lookupService.GetPartitionedTopicMetadata tableViewConfig.Topic.CompleteTopicName
            let! schemaProvider = this.PreProcessSchemaBeforeSubscribe(schema, tableViewConfig.Topic.CompleteTopicName)
            let! activeSchema = getActiveScmema schema tableViewConfig.Topic
            let readerConfig = {
                ReaderConfiguration.Default with
                    Topic = tableViewConfig.Topic
                    StartMessageId = Some MessageId.Earliest
                    ReadCompacted = true
                    AutoUpdatePartitions = tableViewConfig.AutoUpdatePartitions
                    AutoUpdatePartitionsInterval = tableViewConfig.AutoUpdatePartitionsInterval
                }
            let! reader =
                if metadata.IsMultiPartitioned then
                    let consumerInitInfo = {
                        TopicName = tableViewConfig.Topic
                        Schema = activeSchema
                        SchemaProvider = schemaProvider
                        Metadata = metadata
                    }
                    MultiTopicsReaderImpl.Init(readerConfig, config, connectionPool, consumerInitInfo,
                                                             schema, schemaProvider, lookupService)
                else
                    ReaderImpl.Init(readerConfig, config, connectionPool, schema, schemaProvider, lookupService)
            post mb (AddConsumer reader)
            return reader
        }

    member internal this.CreateTableViewAsync<'T> (tableViewConfig: TableViewConfiguration, schema: ISchema<'T>) =
        backgroundTask {
            checkIfActive()
            Log.Logger.LogDebug("CreateTableViewAsync started")
            let! tableView = TableViewImpl.Init((fun () -> this.CreateTableViewReaderAsync(tableViewConfig,schema)))
            return tableView :> ITableView<'T>
        }

    member private this.Mb with get(): Channel<PulsarClientMessage> = mb

    member private this.ClientState
        with get() = Volatile.Read(&clientState)
        and set value = Volatile.Write(&clientState, value)

    member this.IsClosed =
        match this.ClientState with
        | PulsarClientState.Closed | PulsarClientState.Closing -> true
        | _ -> false

    member this.NewProducer() =
        ProducerBuilder(this.CreateProducerAsync, Schema.BYTES())

    member this.NewProducer(schema) =
        ProducerBuilder(this.CreateProducerAsync, schema)

    member this.NewConsumer() =
        ConsumerBuilder(this.SubscribeAsync, this.CreateProducerAsync, Schema.BYTES())

    member this.NewConsumer(schema) =
        ConsumerBuilder(this.SubscribeAsync, this.CreateProducerAsync, schema)

    member this.NewReader() =
        ReaderBuilder(this.CreateReaderAsync, Schema.BYTES())

    member this.NewReader(schema) =
        ReaderBuilder(this.CreateReaderAsync, schema)

    member this.NewTransaction() =
        match transactionClient with
        | Some transClient ->
            TransactionBuilder(transClient)
        | None ->
            failwith "EnableTransaction property is required for starting new transactions"

    member this.NewTableViewBuilder(schema) =
        TableViewBuilder(this.CreateTableViewAsync, schema)
