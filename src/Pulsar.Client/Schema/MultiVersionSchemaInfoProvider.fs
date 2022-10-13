namespace Pulsar.Client.Schema

open System
open System.Text
open System.Threading.Tasks
open Microsoft.Extensions.Caching.Memory

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal MultiVersionSchemaInfoProvider(getSchema : (SchemaVersion -> Task<TopicSchema option>)) =
    let cache = new MemoryCache(MemoryCacheOptions(SizeLimit = Nullable 100000L))


    member this.GetSchemaByVersion (latestSchema: ISchema<'T>, schemaVersion) =
        cache.GetOrCreateAsync<ISchema<'T> option>((latestSchema, schemaVersion), fun (cacheIntry) ->
            backgroundTask {
                cacheIntry.AbsoluteExpirationRelativeToNow <- Nullable <| TimeSpan.FromMinutes(30.0)
                cacheIntry.Size <- Nullable(1L)
                let! schemaReponse = getSchema schemaVersion
                return schemaReponse |> Option.map (fun sch -> latestSchema.GetSpecificSchema(sch.SchemaInfo, sch.SchemaVersion))
            })

    member this.Close() =
        cache.Dispose()