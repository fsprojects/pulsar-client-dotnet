namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open System.Net
open Microsoft.Extensions.Logging

type internal BinaryLookupService (config: PulsarClientConfiguration, connectionPool: ConnectionPool) =

    let endPointResolver = EndPointResolver(config.ServiceAddresses)

    let resolveEndPoint() = endPointResolver.Resolve()

    member this.GetPartitionsForTopic (topicName: TopicName) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata topicName.CompleteTopicName
            if metadata.Partitions > 0
            then
                return Array.init metadata.Partitions (fun i -> topicName.GetPartition(i))
            else
                return [| topicName |]
        }

    member this.GetPartitionedTopicMetadata topicName =
        task {
            let! clientCnx = resolveEndPoint() |> connectionPool.GetBrokerlessConnection
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newPartitionMetadataRequest topicName requestId
            let! response = clientCnx.SendAndWaitForReply requestId payload
            return PulsarResponseType.GetPartitionedTopicMetadata response
        }

    member this.GetBroker(topicName: CompleteTopicName) =
        this.FindBroker(resolveEndPoint(), false, topicName)

    member private this.FindBroker(endpoint: DnsEndPoint, autoritative: bool, topicName: CompleteTopicName) =
        task {
            let! clientCnx = connectionPool.GetBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newLookup topicName requestId autoritative
            let! response = clientCnx.SendAndWaitForReply requestId payload
            let lookupTopicResult = PulsarResponseType.GetLookupTopicResult response
            // (1) build response broker-address
            let uri =
                if config.UseTls then
                    Uri(lookupTopicResult.BrokerServiceUrlTls)
                else
                    Uri(lookupTopicResult.BrokerServiceUrl)

            let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
            // (2) redirect to given address if response is: redirect
            if lookupTopicResult.Redirect then
                Log.Logger.LogDebug("Redirecting to {0} topicName {1}", resultEndpoint, topicName)
                return!  this.FindBroker(resultEndpoint, lookupTopicResult.Authoritative, topicName)
            else
                // (3) received correct broker to connect
                return if lookupTopicResult.Proxy
                       then { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
        }

    member this.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) =
        task {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetTopicsUnderNamespace(resolveEndPoint(), ns, backoff, int config.OperationTimeout.TotalMilliseconds, mode)
            return result
        }

    member private this.GetTopicsUnderNamespace (endpoint: DnsEndPoint, ns: NamespaceName, backoff: Backoff, remainingTimeMs: int, mode: TopicDomain) =
        async {
            try
                let! clientCnx = connectionPool.GetBrokerlessConnection endpoint |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetTopicsOfNamespaceRequest ns requestId mode
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                return (endpoint, response)
            with ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not getTopicsUnderNamespace within configured timeout.")
                Log.Logger.LogWarning(ex, "GetTopicsUnderNamespace failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetTopicsUnderNamespace(endpoint, ns, backoff, remainingTimeMs - delay, mode)
        }
        
    member this.GetSchema(topicName: CompleteTopicName, schemaVersion: SchemaVersion) =
        this.GetSchema(resolveEndPoint(), topicName, Some schemaVersion)
        
    member this.GetSchema(topicName: CompleteTopicName) =
        this.GetSchema(resolveEndPoint(), topicName, None)
        
    member private this.GetSchema(endpoint: DnsEndPoint, topicName: CompleteTopicName, schemaVersion: SchemaVersion option) =
        task {
            let! clientCnx = connectionPool.GetBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newGetSchema topicName requestId schemaVersion
            let! response = clientCnx.SendAndWaitForReply requestId payload
            let schema = PulsarResponseType.GetTopicSchema response
            if schema.IsNone then
                Log.Logger.LogWarning("No schema found for topic {0} version {1}", topicName, schemaVersion)
            return schema
        }