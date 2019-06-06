namespace Pulsar.Client.Internal

open System.Net.Http
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextSensitive
open Utf8Json
open Pulsar.Client.Common
open Utf8Json.Resolvers


type internal HttpLookupService (config: PulsarClientConfiguration, client: HttpClient)   =
    interface ILookupService with
        member this.GetPartitionedTopicMetadata topicName = 
            task {
                let! response = client.GetAsync(sprintf "%s/admin/v2/%s/partitions" config.ServiceUrl topicName)
                let! responseStream = response.Content.ReadAsStreamAsync()
                return! JsonSerializer.DeserializeAsync<PartitionedTopicMetadata>(responseStream, StandardResolver.CamelCase)
            }
