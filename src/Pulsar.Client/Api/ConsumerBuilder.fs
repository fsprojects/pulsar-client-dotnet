namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Threading.Tasks
open FSharp.UMX


type ConsumerBuilder<'T> private (createConsumerAsync, createProducerAsync, config: ConsumerConfiguration<'T>, consumerInterceptors: ConsumerInterceptors<'T>, schema: ISchema<'T>) =

    [<Literal>]
    let MIN_ACK_TIMEOUT_MILLIS = 1000

    [<Literal>]
    let DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER = 30000.0

    let verify(config : ConsumerConfiguration<'T>) =

        config
        |> (fun c ->
                ((c.Topics |> Seq.isEmpty) && String.IsNullOrEmpty(c.TopicsPattern))
                    |> invalidArgIfTrue "Topic name must be set on the consumer builder"
                    |> fun _ -> c
            )
        |> 
            (fun c ->
                c.SubscriptionName
                |> invalidArgIfBlankString "Subscription name must be set on the consumer builder"
                |> fun _ -> c
            )
        |> 
            (fun c ->
                c.Topics
                |> Seq.iter (fun topic ->
                        (c.ReadCompacted && (not topic.IsPersistent ||
                            (c.SubscriptionType <> SubscriptionType.Exclusive && c.SubscriptionType <> SubscriptionType.Failover )))
                        |> invalidArgIfTrue "Read compacted can only be used with exclusive of failover persistent subscriptions"
                        |> ignore
                    )
                c
            )
        |> (fun c ->
                (c.KeySharedPolicy.IsSome && c.SubscriptionType <> SubscriptionType.KeyShared)
                |> invalidArgIfTrue "KeySharedPolicy must be set with KeyShared subscription"
                |> fun _ -> c
            )
        |> (fun c ->
                 (c.BatchReceivePolicy.MaxNumMessages > c.ReceiverQueueSize)
                 |> invalidArgIfTrue "MaxNumMessages can't be greater than ReceiverQueueSize"
                 |> fun _ -> c
            )

    internal new(createConsumerAsync, сreateProducerAsync, schema) = ConsumerBuilder(createConsumerAsync, сreateProducerAsync, ConsumerConfiguration.Default, ConsumerInterceptors.Empty, schema)

    member private this.With(newConfig: ConsumerConfiguration<'T>) =
        ConsumerBuilder(createConsumerAsync, createProducerAsync, newConfig, consumerInterceptors, schema)

    member private this.With(newInterceptors: ConsumerInterceptors<'T>) =
        ConsumerBuilder(createConsumerAsync, createProducerAsync, config, newInterceptors, schema)
    
    member this.Topic topic =
        { config with
            Topics = topic
                |> invalidArgIfBlankString "Topic must not be blank"
                |> fun t -> seq { TopicName(t.Trim()) }
                |> Seq.append config.Topics
                |> Seq.distinct
                |> Seq.cache }
        |> this.With

    member this.Topics (topics: string seq) =
        { config with
            Topics = topics
                |> invalidArgIfDefault "Topics can't be null"
                |> Seq.map (fun t -> TopicName(t.Trim()))
                |> Seq.append config.Topics
                |> Seq.distinct
                |> Seq.cache }
        |> this.With
        
    member this.TopicsPattern pattern =
        { config with
            TopicsPattern = pattern |> invalidArgIfDefault "TopicsPattern must not be blank" }
        |> this.With
    
    member this.ConsumerName name =
        { config with
            ConsumerName = name |> invalidArgIfBlankString "Consumer name must not be blank" }
        |> this.With

    member this.SubscriptionName subscriptionName =
        { config with
            SubscriptionName = subscriptionName |> invalidArgIfBlankString "Subscription name must not be blank" }
        |> this.With        

    member this.SubscriptionType subscriptionType =
        { config with
            SubscriptionType = subscriptionType  }
        |> this.With

    member this.SubscriptionMode subscriptionMode =
        { config with
            SubscriptionMode = subscriptionMode  }
        |> this.With
    
    member this.ReceiverQueueSize receiverQueueSize =
        { config with
            ReceiverQueueSize = receiverQueueSize |> invalidArgIfLessThanZero "ReceiverQueueSize can't be negative."  }
        |> this.With

    member this.SubscriptionInitialPosition subscriptionInitialPosition =
        { config with
            SubscriptionInitialPosition = subscriptionInitialPosition  }
        |> this.With

    member this.AckTimeout ackTimeout =
        { config with
            AckTimeout = ackTimeout |> invalidArgIf (fun arg ->
               arg <> TimeSpan.Zero && arg < TimeSpan.FromMilliseconds(float MIN_ACK_TIMEOUT_MILLIS)) (sprintf "Ack timeout should be greater than %i ms" MIN_ACK_TIMEOUT_MILLIS)  }
        |> this.With

    member this.AckTimeoutTickTime ackTimeoutTickTime =
        { config with
            AckTimeoutTickTime = ackTimeoutTickTime  }
        |> this.With

    member this.AcknowledgementsGroupTime ackGroupTime =
        { config with
            AcknowledgementsGroupTime = ackGroupTime  }
        |> this.With

    member this.ReadCompacted readCompacted =
        { config with
            ReadCompacted = readCompacted  }
        |> this.With

    member this.NegativeAckRedeliveryDelay negativeAckRedeliveryDelay =
        { config with
            NegativeAckRedeliveryDelay = negativeAckRedeliveryDelay  }
        |> this.With

    member this.DeadLettersPolicy (deadLettersPolicy: DeadLettersPolicy) =
        let ackTimeoutTickTime =
            if config.AckTimeoutTickTime = TimeSpan.Zero
            then TimeSpan.FromMilliseconds(DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER)
            else config.AckTimeoutTickTime

        let deadLettersProcessor (topic: TopicName) =
            let getTopicName() = topic.ToString()
            let getSubscriptionName() = config.SubscriptionName
            let createProducer deadLetterTopic =
                ProducerBuilder(createProducerAsync, schema)
                    .Topic(deadLetterTopic)
                    .EnableBatching(false) // dead letters are sent one by one anyway
                    .CreateAsync()
            DeadLettersProcessor(deadLettersPolicy, getTopicName, getSubscriptionName, createProducer) :> IDeadLettersProcessor<'T>

        { config with
            AckTimeoutTickTime = ackTimeoutTickTime
            DeadLettersProcessor = deadLettersProcessor }
        |> this.With

    member this.StartMessageIdInclusive () =
        { config with
            ResetIncludeHead = true }
        |> this.With
        
    member this.AutoUpdatePartitions autoUpdatePartitions =
        { config with
            AutoUpdatePartitions = autoUpdatePartitions }
        |> this.With

    member this.PatternAutoDiscoveryPeriod period =
        { config with
            PatternAutoDiscoveryPeriod = period }
        |> this.With
    
    member this.BatchReceivePolicy (batchReceivePolicy: BatchReceivePolicy) =
        { config with
            BatchReceivePolicy = batchReceivePolicy
                |> invalidArgIfDefault "BatchReceivePolicy can't be null"
                |> fun policy -> policy.Verify(); policy }
        |> this.With

    member this.KeySharedPolicy (keySharedPolicy: KeySharedPolicy) =
        { config with
            KeySharedPolicy = keySharedPolicy
                |> invalidArgIfDefault "KeySharedPolicy can't be null"
                |> fun policy -> keySharedPolicy.Validate(); policy
                |> Some }
        |> this.With

    member this.Intercept ([<ParamArray>] interceptors: IConsumerInterceptor<'T> array) =
        if interceptors.Length = 0 then this
        else
            ConsumerInterceptors(Array.append consumerInterceptors.Interceptors interceptors)
            |> this.With
    
    member this.PriorityLevel (priorityLevel:int) =
        { config with
            PriorityLevel = %(priorityLevel |> invalidArgIfLessThanZero "PriorityLevel can't be negative.") }
        |> this.With
    
    member this.SubscribeAsync(): Task<IConsumer<'T>> =
        createConsumerAsync(verify config, schema, consumerInterceptors)
