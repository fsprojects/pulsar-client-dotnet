namespace Pulsar.Client.UnitTests

open System
open System.Net
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

module internal Defaults =

    let private unused memberName : 'T =
        raise (NotImplementedException $"{memberName} has no default")

    let localBroker =
        let endpoint = DnsEndPoint("127.0.0.1", 6650)
        {
            LogicalAddress = LogicalAddress endpoint
            PhysicalAddress = PhysicalAddress endpoint
        }

    type ClientCnx =
        {
            MaxMessageSize: int
            ClientCnxId: ClientCnxId
            RemoteEndpointProtocolVersion: ProtocolVersion
            Send: SendTask -> Task<bool>
            SendAndForget: SendTask -> unit
            SendAndWaitForReply: RequestId -> SendTask -> Task<PulsarResponseType>
            AddProducer: ProducerId * ProducerOperations -> unit
            RemoveProducer: ProducerId -> unit
            AddConsumer: ConsumerId * ConsumerOperations -> unit
            RemoveConsumer: ConsumerId -> unit
            AddTransactionMetaStoreHandler: TransactionCoordinatorId * TransactionMetaStoreOperations -> unit
            Dispose: unit -> unit
        }

        member cnx.Create() =
            { new IClientCnx with
                member _.MaxMessageSize = cnx.MaxMessageSize
                member _.ClientCnxId = cnx.ClientCnxId
                member _.RemoteEndpointProtocolVersion = cnx.RemoteEndpointProtocolVersion
                member _.Send payload = cnx.Send payload
                member _.SendAndForget payload = cnx.SendAndForget payload
                member _.SendAndWaitForReply requestId payload = cnx.SendAndWaitForReply requestId payload
                member _.AddProducer (producerId, operations) = cnx.AddProducer (producerId, operations)
                member _.RemoveProducer producerId = cnx.RemoveProducer producerId
                member _.AddConsumer (consumerId, operations) = cnx.AddConsumer (consumerId, operations)
                member _.RemoveConsumer consumerId = cnx.RemoveConsumer consumerId
                member _.AddTransactionMetaStoreHandler (coordinatorId, operations) =
                    cnx.AddTransactionMetaStoreHandler (coordinatorId, operations)
                member _.Dispose() = cnx.Dispose() }

    let clientCnx =
        {
            MaxMessageSize = 5 * 1024 * 1024
            ClientCnxId = %0UL
            RemoteEndpointProtocolVersion = ProtocolVersion.V19
            Send = fun _ -> Task.FromResult true
            SendAndForget = ignore
            SendAndWaitForReply = fun _ _ -> unused "SendAndWaitForReply"
            AddProducer = ignore
            RemoveProducer = ignore
            AddConsumer = ignore
            RemoveConsumer = ignore
            AddTransactionMetaStoreHandler = ignore
            Dispose = ignore
        }

    type Lookup =
        {
            GetPartitionsForTopic: TopicName -> Task<TopicName[]>
            GetPartitionedTopicMetadata: CompleteTopicName -> Task<PartitionedTopicMetadata>
            GetBroker: CompleteTopicName -> Task<Broker>
            GetTopicsUnderNamespace: NamespaceName * bool -> Task<string[]>
            GetSchema: CompleteTopicName * SchemaVersion option -> Task<TopicSchema option>
            UpdateServiceInfo: ServiceInfo -> unit
        }

        member lookup.Create() =
            { new ILookupService with
                member _.GetPartitionsForTopic topicName = lookup.GetPartitionsForTopic topicName
                member _.GetPartitionedTopicMetadata topicName = lookup.GetPartitionedTopicMetadata topicName
                member _.GetBroker topicName = lookup.GetBroker topicName
                member _.GetTopicsUnderNamespace (namespaceName, isPersistent) =
                    lookup.GetTopicsUnderNamespace (namespaceName, isPersistent)
                member _.GetSchema (topicName, ?schemaVersion) = lookup.GetSchema (topicName, schemaVersion)
                member _.UpdateServiceInfo serviceInfo = lookup.UpdateServiceInfo serviceInfo
              interface IDisposable with
                member _.Dispose() = () }

    let lookup =
        {
            GetPartitionsForTopic = fun _ -> unused "GetPartitionsForTopic"
            GetPartitionedTopicMetadata = fun _ -> unused "GetPartitionedTopicMetadata"
            GetBroker = fun _ -> Task.FromResult localBroker
            GetTopicsUnderNamespace = fun _ -> unused "GetTopicsUnderNamespace"
            GetSchema = fun _ -> unused "GetSchema"
            UpdateServiceInfo = fun _ -> unused "UpdateServiceInfo"
        }

    let startBytesProducer config connection lookup =
        ProducerImpl.Init(
            config,
            PulsarClientConfiguration.Default,
            (fun _ -> Task.FromResult connection),
            -1,
            lookup,
            Schema.BYTES(),
            ProducerInterceptors<byte[]>.Empty,
            ignore)
