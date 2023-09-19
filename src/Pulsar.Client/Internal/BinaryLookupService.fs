namespace Pulsar.Client.Internal

open Pulsar.Client.Api

open Pulsar.Client.Common
open System
open System.Net
open Microsoft.Extensions.Logging

type internal BinaryLookupService (config: PulsarClientConfiguration, connectionPool: ConnectionPool) =

    let endPointResolver = EndPointResolver(config.ServiceAddresses)

    let resolveEndPoint() = endPointResolver.Resolve()

    member this.GetPartitionsForTopic (topicName: TopicName) =
        backgroundTask {
            let! metadata = this.GetPartitionedTopicMetadata topicName.CompleteTopicName
            if metadata.Partitions > 0 then
                return Array.init metadata.Partitions topicName.GetPartition
            else
                return [| topicName |]
        }

    member private this.GetPartitionedTopicMetadataInner (topicName, backoff: Backoff, remainingTimeMs) =
         async {
            try
                let! clientCnx =
                    resolveEndPoint()
                    |> connectionPool.GetBasicConnection
                    |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newPartitionMetadataRequest topicName requestId
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                return PulsarResponseType.GetPartitionedTopicMetadata response
            with Flatten ex ->
                let nextDelay = Math.Min(backoff.Next(), remainingTimeMs)
                // skip retry scheduler when set lookup throttle in client or server side which will lead to `TooManyRequestsException`
                let isLookupThrottling =
                    (PulsarClientException.isRetriableError ex |> not)
                    || (ex :? TooManyRequestsException)
                if nextDelay <= 0 || isLookupThrottling then
                    reraize ex
                Log.Logger.LogWarning(ex, "GetPartitionedTopicMetadata failed will retry in {0} ms", nextDelay)
                do! Async.Sleep nextDelay
                return! this.GetPartitionedTopicMetadataInner(topicName, backoff, remainingTimeMs - nextDelay)
        }

     member this.GetPartitionedTopicMetadata topicName =
        backgroundTask {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetPartitionedTopicMetadataInner(topicName, backoff, int config.OperationTimeout.TotalMilliseconds)
            return result
        }

    member this.GetBroker(topicName: CompleteTopicName) =
        this.FindBroker(resolveEndPoint(), false, topicName, 0)

    member private this.FindBroker(endpoint: DnsEndPoint, authoritative: bool, topicName: CompleteTopicName,
                                   redirectCount: int) =
        backgroundTask {
            if config.MaxLookupRedirects > 0 && redirectCount > config.MaxLookupRedirects then
                raise (LookupException <| "Too many redirects: " + string redirectCount)
            let! clientCnx = connectionPool.GetBasicConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newLookup topicName requestId authoritative config.ListenerName
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
                return! this.FindBroker(resultEndpoint, lookupTopicResult.Authoritative, topicName, redirectCount + 1)
            else
                // (3) received correct broker to connect
                return if lookupTopicResult.Proxy
                       then { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
        }

    member private this.GetTopicsUnderNamespaceInner (ns: NamespaceName, backoff: Backoff,
                                                 remainingTimeMs: int, isPersistent: bool) =
        async {
            try
                let! clientCnx =
                    resolveEndPoint()
                    |> connectionPool.GetBasicConnection
                    |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetTopicsOfNamespaceRequest ns requestId isPersistent
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                let result = PulsarResponseType.GetTopicsOfNamespace response
                return result
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not getTopicsUnderNamespace within configured timeout.")
                Log.Logger.LogWarning(ex, "GetTopicsUnderNamespace failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetTopicsUnderNamespaceInner(ns, backoff, remainingTimeMs - delay, isPersistent)
        }

    member this.GetTopicsUnderNamespace (ns : NamespaceName, isPersistent : bool) =
        backgroundTask {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetTopicsUnderNamespaceInner(ns, backoff, int config.OperationTimeout.TotalMilliseconds, isPersistent)
            return result
        }

    member private this.GetSchemaInner(topicName: CompleteTopicName, schemaVersion: SchemaVersion option,
                              backoff: Backoff, remainingTimeMs: int) =
        async {
            try
                let! clientCnx =
                    resolveEndPoint()
                    |> connectionPool.GetBasicConnection
                    |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetSchema topicName requestId schemaVersion
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                let schema = PulsarResponseType.GetTopicSchema response
                if schema.IsNone then
                    Log.Logger.LogWarning("No schema found for topic {0} version {1}", topicName, schemaVersion)
                return schema
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not GetSchema within configured timeout.")
                Log.Logger.LogWarning(ex, "GetSchema failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetSchemaInner(topicName, schemaVersion, backoff, remainingTimeMs - delay)
        }

    member this.GetSchema(topicName: CompleteTopicName, ?schemaVersion: SchemaVersion) =
        backgroundTask {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout)
                                        Max = TimeSpan.FromMinutes(1.0) }
            let! result = this.GetSchemaInner(topicName, schemaVersion, backoff, int config.OperationTimeout.TotalMilliseconds)
            return result
        }

