namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open ConnectionPool
open System.Net
open System.Threading.Tasks


type BinaryLookupService (config: PulsarClientConfiguration) =

    let serviceNameResolver = ServiceNameResolver(config)

    let prepareRequest createRequest =
        task {
            let endpoint = serviceNameResolver.ResolveHost()
            let! clientCnx = ConnectionPool.getBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = createRequest requestId
            return (endpoint, fun () -> clientCnx.SendAndWaitForReply requestId payload)
        }

    member __.GetPartitionedTopicMetadata topicName =
        task {
            let makeRequest = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! (_, req) = prepareRequest makeRequest
            return! PulsarTypes.GetPartitionedTopicMetadata req
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()

    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: CompleteTopicName) =
        task {
            let makeRequest = fun requestId -> Commands.newLookup topicName requestId false
            let! (endpoint, req) = prepareRequest makeRequest
            let! lookupTopicResult = PulsarTypes.GetLookupTopicResult req
            let uri = Uri(lookupTopicResult.BrokerServiceUrl)
            let address = DnsEndPoint(uri.Host, uri.Port)
            return if lookupTopicResult.Proxy
                   then { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress endpoint }
                   else { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }
        }

    member __.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) = task {
        let makeRequest = fun requestId -> Commands.newGetTopicsOfNamespaceRequest ns requestId mode
        let! (_, req) = prepareRequest makeRequest
        return! PulsarTypes.GetTopicsOfNamespace req
    }