namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open ConnectionPool
open System.Net
open System.Threading


type BinaryLookupService (config: PulsarClientConfiguration) =

    let serviceNameResolver = ServiceNameResolver(config)
    let mutable retryPartitionedTopicMetadataCount = 0
    let mutable retryGetBrokerCount = 0

    let executeRequest createRequest = task {
        let endpoint = serviceNameResolver.ResolveHost()
        let! clientCnx = ConnectionPool.getBrokerlessConnection endpoint
        let requestId = Generators.getNextRequestId()
        let request = createRequest requestId
        let! response = clientCnx.SendAndWaitForReply requestId request
        return (response, endpoint)
    }

    member __.GetPartitionedTopicMetadata topicName =
        task {
            let makeRequest = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! (response, _) = executeRequest makeRequest
            match response with
            | PartitionedTopicMetadata metadata ->
                retryPartitionedTopicMetadataCount <- 0
                return metadata
            | Error ->
                // TODO backoff
                if (Interlocked.Increment(&retryPartitionedTopicMetadataCount) > 3)
                then
                    return failwith "Couldn't retry GetPartitionedTopicMetadata"
                else
                    return! __.GetPartitionedTopicMetadata(topicName)
            | _ ->
                return failwith "Incorrect return type"
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()

    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: CompleteTopicName) =
        task {
            let makeRequest = fun requestId -> Commands.newLookup topicName requestId false
            let! (response, endpoint) = executeRequest makeRequest
            match response with
            | LookupTopicResult metadata ->
                retryGetBrokerCount <- 0
                let uri = Uri(metadata.BrokerServiceUrl)
                let address = DnsEndPoint(uri.Host, uri.Port)
                return if metadata.Proxy
                       then { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }
            | Error ->
                // TODO backoff
                if (Interlocked.Increment(&retryGetBrokerCount) > 3)
                then
                    return failwith "Couldn't retry GetBroker"
                else
                    return! __.GetBroker(topicName)
            | _ ->
                return failwith "Incorrect return type"
        }

    member __.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) = task {
        let makeRequest = fun requestId -> Commands.newGetTopicsOfNamespaceRequest ns requestId mode
        let! (response, _) = executeRequest makeRequest
        match response with
        | TopicsOfNamespace topics ->
            return topics
        | _ ->
            return failwith "Incorrect return type"
    }