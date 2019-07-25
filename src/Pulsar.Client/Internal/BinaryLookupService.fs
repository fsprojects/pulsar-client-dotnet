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

    let executeRequest createRequest castResponse = task {
        let endpoint = serviceNameResolver.ResolveHost()
        let! clientCnx = ConnectionPool.getBrokerlessConnection endpoint
        let requestId = Generators.getNextRequestId()
        let payload = createRequest requestId

        let! response =
            fun () -> clientCnx.SendAndWaitForReply requestId payload
            |> castResponse
            |> Async.AwaitTask

        return (response, endpoint)
    }

    member __.GetPartitionedTopicMetadata topicName =
        task {
            let makeRequest = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! (response, _) = executeRequest makeRequest PulsarTypes.GetPartitionedTopicMetadata
            return response
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()

    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: CompleteTopicName) =
        task {
            let makeRequest = fun requestId -> Commands.newLookup topicName requestId false
            let! (lookupTopicResult, endpoint) = executeRequest makeRequest PulsarTypes.GetLookupTopicResult
            let uri = Uri(lookupTopicResult.BrokerServiceUrl)
            let address = DnsEndPoint(uri.Host, uri.Port)
            return if lookupTopicResult.Proxy
                   then { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress endpoint }
                   else { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }
        }

    member __.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) = task {
        let makeRequest = fun requestId -> Commands.newGetTopicsOfNamespaceRequest ns requestId mode
        let! (response, _) = executeRequest makeRequest PulsarTypes.GetTopicsOfNamespace
        return response
    }