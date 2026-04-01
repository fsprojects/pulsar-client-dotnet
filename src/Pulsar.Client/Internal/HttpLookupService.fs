namespace Pulsar.Client.Internal

open System.Collections.Generic
open System.IO
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Common
open System
open System.Net
open FSharp.UMX
open System.Text.Json
open Microsoft.Extensions.Logging
open Pulsar.Client.Schema

type internal HttpLookupService (config: PulsarClientConfiguration, _connectionPool: ConnectionPool) =

    let httpClient = new HttpClient(new HttpClientHandler(AllowAutoRedirect = true))
    let jsonOptions = JsonSerializerOptions(
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
     )
    do jsonOptions.Converters.Add(JsonStringEnumConverter())

    interface ILookupService with

        member this.GetPartitionsForTopic (topicName: TopicName) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                let! metadata = this.GetPartitionedTopicMetadataInner(
                    topicName.CompleteTopicName,
                    backoff,
                    int config.OperationTimeout.TotalMilliseconds
                )
                if metadata.Partitions > 0 then
                    return Array.init metadata.Partitions topicName.GetPartition
                else
                    return [| topicName |]
            }

        member this.GetPartitionedTopicMetadata topicName =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                let! result = this.GetPartitionedTopicMetadataInner(topicName, backoff, int config.OperationTimeout.TotalMilliseconds)
                return result
            }

        member this.GetBroker(topicName : CompleteTopicName) =
            this.GetBrokerInner(topicName)

        member this.GetTopicsUnderNamespace (ns: NamespaceName, isPersistent: bool) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                let! result = this.GetTopicsUnderNamespaceInner(ns, backoff, int config.OperationTimeout.TotalMilliseconds, isPersistent)
                return result
            }

        member this.GetSchema(topicName: CompleteTopicName, ?schemaVersion: SchemaVersion) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                let! result = this.GetSchemaInner(topicName, schemaVersion, backoff, int config.OperationTimeout.TotalMilliseconds)
                return result
            }

    member private _.GetJsonResponse<'T>(path: string) =
        task {
            use! response = httpClient.GetAsync(path)
            response.EnsureSuccessStatusCode() |> ignore
            use! stream = response.Content.ReadAsStreamAsync()
            return JsonSerializer.Deserialize<'T>(stream, jsonOptions)
        }

    //  GET /admin/v2/{topic-domain}/{tenant}/{namespace}/{topic}/partitions?checkAllowAutoCreation=true
    member private this.GetPartitionedTopicMetadataInner (topicName: CompleteTopicName, backoff: Backoff, remainingTimeMs) =
         async {
            try
                let randomServiceUri = config.ServiceAddresses[RandomGenerator.Next(0, config.ServiceAddresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://","persistent/").Replace("non-persistent://","non-persistent/")
                let! response = randomServiceUri.AbsoluteUri + $"admin/v2/%s{topicRestPath}/partitions?checkAllowAutoCreation=true"
                                |> this.GetJsonResponse<{| Partitions: int |}>
                                |> Async.AwaitTask
                return { Partitions = response.Partitions }

            with Flatten ex ->
                let nextDelay = Math.Min(backoff.Next(), remainingTimeMs)
                if nextDelay <= 0 then
                    reraize ex
                Log.Logger.LogWarning(ex, "GetPartitionedTopicMetadata failed will retry in {0} ms", nextDelay)
                do! Async.Sleep nextDelay
                return! this.GetPartitionedTopicMetadataInner(topicName, backoff, remainingTimeMs - nextDelay)
        }

    //  GET /lookup/v2/topic/{topic-domain}/{tenant}/{namespace}/{topic}
    member private this.GetBrokerInner(topicName: CompleteTopicName) =
        backgroundTask {
            let randomServiceUri = config.ServiceAddresses[RandomGenerator.Next(0, config.ServiceAddresses.Length)]
            let topic: string = %topicName
            let topicRestPath = topic.Replace("persistent://","persistent/").Replace("non-persistent://","non-persistent/")
            let! response =
                this.GetJsonResponse<{|
                    brokerUrl : string
                    brokerUrlTls: string
                    httpUrl: string
                    httpUrlTls: string
                |}> (randomServiceUri.AbsoluteUri + $"lookup/v2/topic/%s{topicRestPath}")
            let uri = if config.UseTls
                      then Uri(response.brokerUrlTls)
                      else Uri(response.brokerUrl)
            let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
            return { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
        }

    //  GET /admin/v2/namespaces/{tenant}/{namespace}/topics?mode=PERSISTENT
    //  GET /admin/v2/namespaces/{tenant}/{namespace}/topics?mode=NON_PERSISTENT
    member private this.GetTopicsUnderNamespaceInner (ns: NamespaceName, backoff: Backoff, remainingTimeMs: int, isPersistent: bool) =
        async {
            try
                let randomServiceUri = config.ServiceAddresses[RandomGenerator.Next(0, config.ServiceAddresses.Length)]
                let mode = match isPersistent with
                            | true -> "PERSISTENT"
                            | false -> "NON_PERSISTENT"
                let! response = randomServiceUri.AbsoluteUri + $"admin/v2/namespaces/%s{ns.ToString()}/topics?mode=%s{mode}"
                                |> this.GetJsonResponse<string seq>
                                |> Async.AwaitTask
                return response

            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not getTopicsUnderNamespace within configured timeout.")
                Log.Logger.LogWarning(ex, "GetTopicsUnderNamespace failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetTopicsUnderNamespaceInner(ns, backoff, remainingTimeMs - delay, isPersistent)
        }

    //  GET /admin/v2/schemas/{tenant}/{namespace}/{topic}/schema
    //  GET /admin/v2/schemas/{tenant}/{namespace}/{topic}/schema/{version}
    member private this.GetSchemaInner(topicName: CompleteTopicName, schemaVersion: SchemaVersion option,
                              backoff: Backoff, remainingTimeMs: int) =
         async {
            try
                let randomServiceUri = config.ServiceAddresses[RandomGenerator.Next(0, config.ServiceAddresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://","").Replace("non-persistent://","")
                let path = match schemaVersion with
                            | Some sv ->
                                //  read 8 bytes from big endian
                                let binaryWriter = new BinaryReader(new MemoryStream(sv.Bytes))
                                let schemaVersionInt = binaryWriter.ReadInt64() |> int64FromBigEndian
                                $"admin/v2/schemas/%s{topicRestPath}/schema/%d{schemaVersionInt}"
                            | None ->
                                $"admin/v2/schemas/%s{topicRestPath}/schema"
                use! responseMessage = httpClient.GetAsync(randomServiceUri.AbsoluteUri + path) |> Async.AwaitTask
                if responseMessage.StatusCode = HttpStatusCode.NotFound then
                    Log.Logger.LogWarning("No schema found for topic {0} version {1}", topicName, schemaVersion)
                    return None
                else
                    responseMessage.EnsureSuccessStatusCode() |> ignore
                    use! response = responseMessage.Content.ReadAsStreamAsync() |> Async.AwaitTask
                    let schemaResponse = JsonSerializer.Deserialize<
                                        {|
                                          Version : Int64
                                          Type: SchemaType
                                          Timestamp: Int64
                                          Data: string
                                          Properties: Dictionary<string, string>
                                        |}>(response, jsonOptions)

                    let schemaData = match schemaResponse.Type with
                                     | SchemaType.KEY_VALUE -> schemaResponse.Data |> this.getKeyValueSchemaBytes
                                     | _ -> schemaResponse.Data |> System.Text.Encoding.UTF8.GetBytes
                    let schemaVersion: SchemaVersion = {
                        Bytes = schemaResponse.Version |> BitConverter.GetBytes |> Array.rev
                    }
                    let schemaInfo: SchemaInfo = {
                        Name = topicRestPath
                        Type = schemaResponse.Type
                        Properties = schemaResponse.Properties
                        Schema = schemaData
                    }
                    let topicSchema: TopicSchema = {
                        SchemaVersion = Some schemaVersion
                        SchemaInfo = schemaInfo
                    }
                    return Some topicSchema

            with
                | Flatten ex ->
                    let delay = Math.Min(backoff.Next(), remainingTimeMs)
                    if delay <= 0 then
                        raise (TimeoutException "Could not GetSchema within configured timeout.")
                    Log.Logger.LogWarning(ex, "GetSchema failed will retry in {0} ms", delay)
                    do! Async.Sleep delay
                    return! this.GetSchemaInner(topicName, schemaVersion, backoff, remainingTimeMs - delay)
        }

    //  Convert key/value schema json string to schema bytes[]
    member private this.getKeyValueSchemaBytes(keyValueDataString: string) =
        //  Key-value type schema `Data` field is a complicated embedded json string
        //  so that it's difficult to deserialize the field by orm method.
        //  Instead, we only need to extract 'key' and 'value' field and call `KeyValueSchema.GetKeyValueBytes` method.
        let json = JsonDocument.Parse(keyValueDataString)
        let keyString = json.RootElement.GetProperty("key").GetRawText()
        let valueString = json.RootElement.GetProperty("value").GetRawText()
        KeyValueSchema.GetKeyValueBytes(
            keyString |> System.Text.Encoding.UTF8.GetBytes,
            valueString |> System.Text.Encoding.UTF8.GetBytes
        )
