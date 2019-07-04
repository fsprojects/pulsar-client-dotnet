namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open SocketManager
open System.Net


type BinaryLookupService (config: PulsarClientConfiguration) =

    let serviceNameResolver = ServiceNameResolver(config)

    let executeRequest createRequest = task {
        let endpoint = serviceNameResolver.ResolveHost()
        let! connection = SocketManager.getBrokerlessConnection endpoint
        let requestId = Generators.getNextRequestId()
        let request = createRequest requestId
        let! response = SocketManager.sendAndWaitForReply requestId (connection, request)
        return (response, endpoint)
    }

    member __.GetPartitionedTopicMetadata topicName = 
        task {
            let makeRequest = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! (response, _) = executeRequest makeRequest
            match response with
            | PartitionedTopicMetadata metadata ->
                return metadata
            | _ -> 
                return failwith "Incorrect return type"
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()
 
    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topic: string) = 
        task {
            let topicName = TopicName topic
            return! __.GetBroker(topicName)
        }

    member __.GetBroker(topicName: TopicName) = 
        task {
            let makeRequest = fun requestId -> Commands.newLookup (topicName.ToString()) requestId false
            let! (response, endpoint) = executeRequest makeRequest
            match response with
            | LookupTopicResult metadata ->
                let uri = Uri(metadata.BrokerServiceUrl)
                let address = DnsEndPoint(uri.Host, uri.Port)
                return if metadata.Proxy
                       then { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress address }
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