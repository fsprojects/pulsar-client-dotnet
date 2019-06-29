namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pipelines.Sockets.Unofficial
open System.Buffers
open System.Text
open Pulsar.Client.Common
open System
open Utf8Json
open System.Threading.Tasks
open SocketManager
open System.Net


type BinaryLookupService (config: PulsarClientConfiguration) =
    let serviceNameResolver = ServiceNameResolver(config)

    member __.GetPartitionedTopicMetadata topicName = 
        task {
            let endpoint = serviceNameResolver.ResolveHost()
            let! connection = SocketManager.getBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let request = 
                Commands.newPartitionMetadataRequest topicName requestId
            let! result = SocketManager.sendAndWaitForReply requestId (connection, request)
            match result with
            | PartitionedTopicMetadata metadata ->
                return metadata
            | _ -> 
                return failwith "Incorrect return type"
        }

    member __.GetServiceUrl() = serviceNameResolver.GetServiceUrl()
 
    member __.UpdateServiceUrl(serviceUrl) = serviceNameResolver.UpdateServiceUrl(serviceUrl)

    member __.GetBroker(topicName: TopicName) = 
        task {
            let initialEndpoint = serviceNameResolver.ResolveHost()
            let! connection = SocketManager.getBrokerlessConnection initialEndpoint
            let requestId = Generators.getNextRequestId()
            let request = Commands.newLookup (topicName.ToString()) requestId false
            let! result = SocketManager.sendAndWaitForReply requestId (connection, request)
            match result with
            | LookupTopicResult metadata ->
                let uri = Uri(metadata.BrokerServiceUrl)
                let address = DnsEndPoint(uri.Host, uri.Port)
                return if metadata.Proxy
                       then { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress initialEndpoint }
                       else { LogicalAddress = LogicalAddres address; PhysicalAddress = PhysicalAddress address }
            | _ -> 
                return failwith "Incorrect return type"
        }