namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open System.Security.Authentication
open System.Security.Cryptography.X509Certificates

type PulsarClientConfiguration =
    {
        ServiceAddresses: Uri list
        OperationTimeout: TimeSpan
        MaxNumberOfRejectedRequestPerConnection: int
        UseTls: bool
        TlsHostnameVerificationEnable: bool
        TlsAllowInsecureConnection: bool
        TlsTrustCertificate: X509Certificate2
        Authentication: Authentication
        TlsProtocols: SslProtocols
    }
    static member Default =
        {
            ServiceAddresses = List.empty<Uri>
            OperationTimeout = TimeSpan.FromMilliseconds(30000.0)
            MaxNumberOfRejectedRequestPerConnection = 50
            UseTls = false
            TlsHostnameVerificationEnable = false
            TlsAllowInsecureConnection = false
            TlsTrustCertificate = null
            Authentication = Authentication.AuthenticationDisabled
            TlsProtocols = SslProtocols.None
        }

type ConsumerConfiguration =
    {
        Topic: TopicName
        ConsumerName: string
        SubscriptionName: string
        SubscriptionType: SubscriptionType
        SubscriptionMode: SubscriptionMode
        ReceiverQueueSize: int
        MaxTotalReceiverQueueSizeAcrossPartitions: int
        SubscriptionInitialPosition: SubscriptionInitialPosition
        AckTimeout: TimeSpan
        AckTimeoutTickTime: TimeSpan
        AcknowledgementsGroupTime: TimeSpan
        AutoUpdatePartitions: bool
        ReadCompacted: bool
        NegativeAckRedeliveryDelay: TimeSpan
        ResetIncludeHead: bool
        DeadLettersProcessor : IDeadLettersProcessor
        KeySharedPolicy: KeySharedPolicy option
        BatchReceivePolicy: BatchReceivePolicy
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            SubscriptionMode = SubscriptionMode.Durable
            ReceiverQueueSize = 1000
            MaxTotalReceiverQueueSizeAcrossPartitions = 50000
            SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
            AckTimeout = TimeSpan.Zero
            AckTimeoutTickTime = TimeSpan.FromMilliseconds(1000.0)
            AcknowledgementsGroupTime = TimeSpan.FromMilliseconds(100.0)
            AutoUpdatePartitions = true
            ReadCompacted = false
            NegativeAckRedeliveryDelay = TimeSpan.FromMinutes(1.0)
            ResetIncludeHead = false
            DeadLettersProcessor = DeadLettersProcessor.Disabled
            KeySharedPolicy = None
            BatchReceivePolicy = BatchReceivePolicy()
        }

type ProducerConfiguration =
    {
        Topic: TopicName
        ProducerName: string
        MaxPendingMessagesAcrossPartitions: int
        MaxPendingMessages: int
        BatchingEnabled: bool
        BatchingMaxMessages: int
        BatchingMaxBytes: int
        BatchingMaxPublishDelay: TimeSpan
        BatchingPartitionSwitchFrequencyByPublishDelay: int
        BatchBuilder: BatchBuilder
        SendTimeout: TimeSpan
        CompressionType: CompressionType
        MessageRoutingMode: MessageRoutingMode
        CustomMessageRouter: IMessageRouter option
        AutoUpdatePartitions: bool
        HashingScheme: HashingScheme
    }
    member this.BatchingPartitionSwitchFrequencyIntervalMs =
        this.BatchingPartitionSwitchFrequencyByPublishDelay * (int this.BatchingMaxPublishDelay.TotalMilliseconds)
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ProducerName = ""
            MaxPendingMessages = 1000
            MaxPendingMessagesAcrossPartitions = 50000
            BatchingEnabled = true
            BatchingMaxMessages = 1000
            BatchingMaxBytes = 128 * 1024 // 128KB
            BatchingMaxPublishDelay = TimeSpan.FromMilliseconds(1.0)
            BatchingPartitionSwitchFrequencyByPublishDelay = 10
            BatchBuilder = BatchBuilder.Default
            SendTimeout = TimeSpan.FromMilliseconds(30000.0)
            CompressionType = CompressionType.None
            MessageRoutingMode = MessageRoutingMode.RoundRobinPartition
            CustomMessageRouter = None
            AutoUpdatePartitions = true
            HashingScheme = HashingScheme.DotnetStringHash
        }

type ReaderConfiguration =
    {
        Topic: TopicName
        StartMessageId: MessageId option
        SubscriptionRolePrefix: string
        ReceiverQueueSize: int
        ReadCompacted: bool
        ReaderName: string
        ResetIncludeHead: bool
        StartMessageFromRollbackDuration: TimeSpan
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            StartMessageId = None
            SubscriptionRolePrefix = ""
            ReceiverQueueSize = 1000
            ReadCompacted = false
            ReaderName = ""
            ResetIncludeHead = false
            StartMessageFromRollbackDuration = TimeSpan.Zero
        }
