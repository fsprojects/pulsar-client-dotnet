namespace Pulsar.Client.Schema

open System
open System.Text
open System.Threading.Tasks
open Microsoft.Extensions.Caching.Memory
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal MultiVersionSchemaInfoProvider(getSchema : (SchemaVersion -> Task<TopicSchema option>)) =
    let cache = new MemoryCache(MemoryCacheOptions(SizeLimit = Nullable 100000L))
    
    
    member this.GetSchemaByVersion (schema: ISchema<'T>, schemaVersion) =
        cache.GetOrCreateAsync<ISchema<'T> option>(schemaVersion, fun (cacheIntry) ->
            task {                
                cacheIntry.AbsoluteExpirationRelativeToNow <- Nullable <| TimeSpan.FromMinutes(30.0)
                cacheIntry.Size <- Nullable(1L)
                let! schemaReponse = getSchema schemaVersion
                return schemaReponse |> Option.map (fun sch -> schema.GetSpecificSchema(sch.SchemaInfo.Schema |> Encoding.UTF8.GetString))
            })
        
    member this.Close() =
        cache.Dispose()