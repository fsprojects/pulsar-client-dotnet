namespace Pulsar.Client.Internal

open System.Net.Http
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Utf8Json
open Pulsar.Client.Common
open Utf8Json.Resolvers


type internal HttpLookupService (config: PulsarClientConfiguration) =

    let mutable resolver = ServiceNameResolver(config)
    let client = new HttpClient(BaseAddress = resolver.ResolveHostUri())

    interface ILookupService with

        member __.GetPartitionedTopicMetadata topicName = 
            task {
                let! response = client.GetAsync(sprintf "%s/admin/v2/%s/partitions" config.ServiceUrl topicName)
                let! responseStream = response.Content.ReadAsStreamAsync()
                return! JsonSerializer.DeserializeAsync<PartitionedTopicMetadata>(responseStream, StandardResolver.CamelCase)
            }

        member __.GetServiceUrl(): string = resolver.GetServiceUrl()

        member __.UpdateServiceUrl(serviceUrl) =
            resolver <- resolver.UpdateServiceUrl(serviceUrl)
            client.BaseAddress <- resolver.ResolveHostUri()
            
        member this.GetBroker(topicName: TopicName) = 
            raise (System.NotImplementedException())
