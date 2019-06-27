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


type BinaryLookupService (config: PulsarClientConfiguration) =
    let serviceNameResolver = ServiceNameResolver(config)

    member __.GetPartitionedTopicMetadata topicName = 
        task {
            let endpoint = serviceNameResolver.ResolveHost()
            let! connection = SocketManager.getConnection { PhysicalAddress = endpoint; LogicalAddress = endpoint }
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

    member __.GetBroker(topicName: TopicName): Task<Broker> = 
        raise (System.NotImplementedException())