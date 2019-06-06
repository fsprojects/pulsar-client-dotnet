namespace Pulsar.Client.Internal

open System.Net.Http
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextSensitive
open Utf8Json
open Pulsar.Client.Common
open Utf8Json.Resolvers
open System.Threading.Tasks


type internal BinaryProtoLookupService (config: PulsarClientConfiguration)   =
    interface ILookupService with
        member this.GetPartitionedTopicMetadata topicName = 
            Task.FromResult({ Partitions = 1 })
