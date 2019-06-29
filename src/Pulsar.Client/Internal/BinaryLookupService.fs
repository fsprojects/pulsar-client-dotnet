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
        return {| Response = response; Enpoint = endpoint |}
    }

    member __.GetPartitionedTopicMetadata topicName = 
        task {
            let makeRequest = fun requestId -> Commands.newPartitionMetadataRequest topicName requestId
            let! result = executeRequest makeRequest
            match result.Response with
            | PartitionedTopicMetadata metadata ->
                return metadata
            | _ -> 
                return failwith "Incorrect return type"
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()
 
    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: TopicName) = 
        task {
            let makeRequest = fun requestId -> Commands.newLookup (topicName.ToString()) requestId false
            let! result = executeRequest makeRequest
            match result.Response with
            | LookupTopicResult metadata ->
                let uri = Uri(metadata.BrokerServiceUrl)
                let address = DnsEndPoint(uri.Host, uri.Port)
                return if metadata.Proxy
                       then { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress result.Enpoint }
                       else { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress address }
            | _ -> 
                return failwith "Incorrect return type"
        }

    member __.GetTopicsUnderNamespace (ns : string, mode : TopicDomain) = task {
        let makeRequest = fun requestId -> Commands.newGetTopicsOfNamespaceRequest ns requestId mode
        let! result = executeRequest makeRequest
        match result.Response with
        | TopicsOfNamespace topics ->
            return topics
        | _ -> 
            return failwith "Incorrect return type"
    }