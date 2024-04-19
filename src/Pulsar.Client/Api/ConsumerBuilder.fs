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

    let deadLettersProcessor (c: ConsumerConfiguration<'T>) (deadLettersPolicy: DeadLetterPolicy) (topic: TopicName)  =
        let getTopicName () =
            topic.ToString()
        let createProducer deadLetterTopic =
            ProducerBuilder(createProducerAsync, schema)
                .Topic(deadLetterTopic)
                .BlockIfQueueFull(false)
                .CreateAsync()
        DeadLetterProcessor(deadLettersPolicy, getTopicName, c.SubscriptionName, createProducer) :> IDeadLetterProcessor<'T>
    
    let verify(config : ConsumerConfiguration<'T>) =
        config
        |> invalidArgIf (fun c ->
                ((c.Topics |> Seq.isEmpty) && String.IsNullOrEmpty(c.TopicsPattern))
            ) "Topic name must be set on the consumer builder"
        |> (fun c ->
                %c.SubscriptionName
                |> invalidArgIfBlankString "Subscription name must be set on the consumer builder"
                |> fun _ -> c
            )
        |> invalidArgIf (fun c ->
                c.Topics
                |> Seq.tryFind (fun topic ->
                        (c.ReadCompacted && (not topic.IsPersistent ||
                            (c.SubscriptionType <> SubscriptionType.Exclusive && c.SubscriptionType <> SubscriptionType.Failover )))
                    )
                |> Option.isSome
            ) "Read compacted can only be used with exclusive or failover persistent subscriptions"
        |> invalidArgIf (fun c ->
                (c.KeySharedPolicy.IsSome && c.SubscriptionType <> SubscriptionType.KeyShared)
            ) "KeySharedPolicy must be set with KeyShared subscription"
        |> invalidArgIf (fun c ->
                 (c.BatchReceivePolicy.MaxNumMessages > c.ReceiverQueueSize)
            ) "MaxNumMessages can't be greater than ReceiverQueueSize"
        |> (fun c ->
                match c.DeadLetterPolicy with
                | Some _ when c.AckTimeout= TimeSpan.Zero ->
                    { c with
                        AckTimeout = TimeSpan.FromMilliseconds(DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER)}
                | _ ->
                    c
            )
        |> (fun c ->
                if c.RetryEnable && (c.Topics |> Seq.isEmpty |> not) then
                    let prefixPart = c.SingleTopic.ToString() + "-" + %c.SubscriptionName
                    let defaultRetryLetterTopic = prefixPart + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX
                    let defaultDeadLetterTopic = prefixPart + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX
                    let newPolicy =
                        match c.DeadLetterPolicy with
                        | None ->
                            DeadLetterPolicy(RetryMessageUtil.MAX_RECONSUMETIMES, defaultDeadLetterTopic, defaultRetryLetterTopic)
                        | Some policy ->
                            let isEmptyDL = String.IsNullOrEmpty policy.DeadLetterTopic
                            let isEmptyRL = String.IsNullOrEmpty policy.RetryLetterTopic
                            if isEmptyDL && isEmptyRL then
                                DeadLetterPolicy(policy.MaxRedeliveryCount, defaultDeadLetterTopic, defaultRetryLetterTopic)
                            elif isEmptyDL then
                                DeadLetterPolicy(policy.MaxRedeliveryCount, defaultDeadLetterTopic, policy.RetryLetterTopic)
                            elif isEmptyRL then
                                DeadLetterPolicy(policy.MaxRedeliveryCount, policy.DeadLetterTopic, defaultRetryLetterTopic)
                            else
                                policy
                    { c with
                        DeadLetterProcessor = deadLettersProcessor c newPolicy
                        DeadLetterPolicy = Some newPolicy
                        Topics = seq { yield! c.Topics; yield TopicName(newPolicy.RetryLetterTopic) } |> Seq.cache }
                else
                    c
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
            SubscriptionName = %(subscriptionName |> invalidArgIfBlankString "Subscription name must not be blank") }
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

    member this.DeadLetterPolicy (deadLetterPolicy: DeadLetterPolicy) =
        { config with
            DeadLetterProcessor = deadLettersProcessor config deadLetterPolicy
            DeadLetterPolicy = Some deadLetterPolicy  }
        |> this.With

    member this.StartMessageIdInclusive () =
        { config with
            ResetIncludeHead = true }
        |> this.With
        
    member this.AutoUpdatePartitions autoUpdatePartitions =
        { config with
            AutoUpdatePartitions = autoUpdatePartitions }
        |> this.With
        
    member this.AutoUpdateInterval autoUpdatePartitionsInterval =
        { config with
            AutoUpdatePartitionsInterval = autoUpdatePartitionsInterval }
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
        if interceptors.Length = 0 then
            this
        else
            ConsumerInterceptors(Array.append consumerInterceptors.Interceptors interceptors)
            |> this.With
    
    member this.PriorityLevel (priorityLevel:int) =
        { config with
            PriorityLevel = %(priorityLevel |> invalidArgIfLessThanZero "PriorityLevel can't be negative.") }
        |> this.With
        
     member this.EnableRetry retryEnable =
        { config with
            RetryEnable = retryEnable }
        |> this.With
        
    member this.EnableBatchIndexAcknowledgment enableBatchIndexAcknowledgment =
        { config with
            BatchIndexAcknowledgmentEnabled = enableBatchIndexAcknowledgment }
        |> this.With
    
    member this.MaxPendingChunkedMessage maxPendingChunkedMessage =
        { config with
            MaxPendingChunkedMessage = maxPendingChunkedMessage }
        |> this.With
        
    member this.AutoAckOldestChunkedMessageOnQueueFull autoAckOldestChunkedMessageOnQueueFull =
        { config with
            AutoAckOldestChunkedMessageOnQueueFull = autoAckOldestChunkedMessageOnQueueFull }
        |> this.With
        
    member this.ExpireTimeOfIncompleteChunkedMessage expireTimeOfIncompleteChunkedMessage =
        { config with
            ExpireTimeOfIncompleteChunkedMessage = expireTimeOfIncompleteChunkedMessage }
        |> this.With
    
    member this.MessageDecryptor messageDecryptor  =
        { config with
            MessageDecryptor = Some messageDecryptor }
        |> this.With

    member this.CryptoFailureAction (action: ConsumerCryptoFailureAction)  =
        { config with
            ConsumerCryptoFailureAction = action }
        |> this.With
        
    member this.ReplicateSubscriptionState replicateSubscriptionState =
        { config with
            ReplicateSubscriptionState = replicateSubscriptionState }
        |> this.With        
    
    member this.SubscribeAsync(): Task<IConsumer<'T>> =
        createConsumerAsync(verify config, schema, consumerInterceptors)
        
    member this.Configuration =
        config
