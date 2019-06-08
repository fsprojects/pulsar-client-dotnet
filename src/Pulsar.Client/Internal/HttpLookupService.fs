namespace Pulsar.Client.Internal

open System.Net.Http
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Utf8Json
open Pulsar.Client.Common
open Utf8Json.Resolvers
open System


type internal HttpLookupService (config: PulsarClientConfiguration) =
    let client = new HttpClient(BaseAddress = Uri(config.ServiceUrl))

    interface ILookupService with
        member this.GetPartitionedTopicMetadata topicName = 
            task {
                let! response = client.GetAsync(sprintf "%s/admin/v2/%s/partitions" config.ServiceUrl topicName)
                let! responseStream = response.Content.ReadAsStreamAsync()
                return! JsonSerializer.DeserializeAsync<PartitionedTopicMetadata>(responseStream, StandardResolver.CamelCase)
            }
