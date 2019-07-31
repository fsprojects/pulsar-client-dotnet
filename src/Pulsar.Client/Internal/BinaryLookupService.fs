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

    let makeRequest getPayload =
        task {
            let endpoint = serviceNameResolver.ResolveHost()
            let! clientCnx = ConnectionPool.getBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = getPayload requestId
            let! response = clientCnx.SendAndWaitForReply requestId payload
            return (endpoint, response)
        }

    member __.GetPartitionedTopicMetadata topicName =
        task {
            let getPayload = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! (_, response) = makeRequest getPayload
            return PulsarResponseType.GetPartitionedTopicMetadata response
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()

    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: CompleteTopicName) =
        task {
            let getPayload = fun requestId -> Commands.newLookup topicName requestId false
            let! (endpoint, response) = makeRequest getPayload
            let lookupTopicResult = PulsarResponseType.GetLookupTopicResult response
            let uri = Uri(lookupTopicResult.BrokerServiceUrl)
            let address = DnsEndPoint(uri.Host, uri.Port)
            return if lookupTopicResult.Proxy
                   then { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress endpoint }
                   else { LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address }
        }

    member __.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) = task {
        let getPayload = fun requestId -> Commands.newGetTopicsOfNamespaceRequest ns requestId mode
        let! (_, response) = makeRequest getPayload
        return PulsarResponseType.GetTopicsOfNamespace response
    }