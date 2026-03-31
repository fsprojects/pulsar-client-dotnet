namespace Pulsar.Client.Internal

open System.Collections.Generic
open System.IO
open System.Net.Http
open Pulsar.Client.Api
open Pulsar.Client.Common
open System
open System.Net
open System.Threading
open FSharp.UMX
open System.Text.Json
open Microsoft.Extensions.Logging
open Pulsar.Client.Schema
open System.Net.Http.Json
open System.Text.Json.Serialization

//  This class is mainly used for http lookup service
//  We name this class `PulsarHttpClient` to avoid naming clash with native HttpClient, and in Java pulsar client it's just `HttpClient`
type internal PulsarHttpClient () =

    let jsonOptions = JsonSerializerOptions(
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
     )
    do jsonOptions.Converters.Add(JsonStringEnumConverter())

    let httpClient = new HttpClient(new SocketsHttpHandler(
        PooledConnectionLifetime = TimeSpan.FromMinutes(2),
        AllowAutoRedirect = true
    ))

    member this.Get<'T> (requestUri: string, auth: Authentication) =
        backgroundTask {
            let authenticationDataProvider = auth.GetAuthData()
            if authenticationDataProvider.HasDataForHttp() then
                let request = new HttpRequestMessage(HttpMethod.Get, requestUri)
                for headerPropertyEntry in authenticationDataProvider.GetHttpHeaders() do
                    request.Headers.Add(headerPropertyEntry.Key, headerPropertyEntry.Value)
                let! response = httpClient.SendAsync(request)
                response.EnsureSuccessStatusCode() |> ignore
                return! response.Content.ReadFromJsonAsync<'T>(jsonOptions)
            else
                return! httpClient.GetFromJsonAsync<'T>(requestUri, jsonOptions)
        }

    member this.Dispose() =
        httpClient.Dispose()


type internal HttpLookupService (config: PulsarClientConfiguration) =

    let pulsarHttpClient = PulsarHttpClient()
    let mutable currentServiceInfo =
        ServiceInfo({
            OriginalString = "" // not used here
            Addresses = config.ServiceAddresses
            UseTls = config.UseTls
            Scheme = config.Scheme
        }, config.Authentication, config.TlsTrustCertificate)

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

        //  GET /lookup/v2/topic/{topic-domain}/{tenant}/{namespace}/{topic}
        member this.GetBroker(topicName : CompleteTopicName) =
            backgroundTask {
                let serviceInfo = Volatile.Read(&currentServiceInfo)
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://","persistent/").Replace("non-persistent://","non-persistent/")
                let url = randomServiceUri.AbsoluteUri + $"lookup/v2/topic/%s{topicRestPath}"
                let! brokerResponse =
                    pulsarHttpClient.Get<{| BrokerUrl : string;
                                               BrokerUrlTls: string;
                                               HttpUrl: string;
                                               HttpUrlTls: string |}>(url, serviceInfo.Authentication)
                let uri =
                    if serviceInfo.ServiceUrl.UseTls then
                        Uri(brokerResponse.BrokerUrlTls)
                    else
                        Uri(brokerResponse.BrokerUrl)
                let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
                return { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
            }

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

        member this.UpdateServiceInfo(serviceInfo: ServiceInfo) =
            Volatile.Write(&currentServiceInfo, serviceInfo)

        member this.Dispose() =
            pulsarHttpClient.Dispose()

    //  GET /admin/v2/{topic-domain}/{tenant}/{namespace}/{topic}/partitions?checkAllowAutoCreation=true
    member private this.GetPartitionedTopicMetadataInner (topicName: CompleteTopicName, backoff: Backoff, remainingTimeMs) =
         async {
            try
                let serviceInfo = Volatile.Read(&currentServiceInfo)
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://","persistent/").Replace("non-persistent://","non-persistent/")
                let url = randomServiceUri.AbsoluteUri + $"admin/v2/%s{topicRestPath}/partitions?checkAllowAutoCreation=true"
                let! brokerResponse =
                    pulsarHttpClient.Get<{| Partitions: int |}>(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                return { Partitions = brokerResponse.Partitions }

            with Flatten ex ->
                let nextDelay = Math.Min(backoff.Next(), remainingTimeMs)
                if nextDelay <= 0 then
                    reraize ex
                Log.Logger.LogWarning(ex, "GetPartitionedTopicMetadata failed will retry in {0} ms", nextDelay)
                do! Async.Sleep nextDelay
                return! this.GetPartitionedTopicMetadataInner(topicName, backoff, remainingTimeMs - nextDelay)
        }

    //  GET /admin/v2/namespaces/{tenant}/{namespace}/topics?mode=PERSISTENT
    //  GET /admin/v2/namespaces/{tenant}/{namespace}/topics?mode=NON_PERSISTENT
    member private this.GetTopicsUnderNamespaceInner (ns: NamespaceName, backoff: Backoff, remainingTimeMs: int, isPersistent: bool) =
        async {
            try
                let serviceInfo = Volatile.Read(&currentServiceInfo)
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let mode =
                    match isPersistent with
                    | true -> "PERSISTENT"
                    | false -> "NON_PERSISTENT"
                let url = randomServiceUri.AbsoluteUri + $"admin/v2/namespaces/%s{ns.ToString()}/topics?mode=%s{mode}"
                let! brokerResponse =
                    pulsarHttpClient.Get<string[]>(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                return brokerResponse

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
                let serviceInfo = Volatile.Read(&currentServiceInfo)
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://","").Replace("non-persistent://","")
                let path =
                    match schemaVersion with
                    | Some sv ->
                        //  read 8 bytes from big endian
                        let binaryWriter = new BinaryReader(new MemoryStream(sv.Bytes))
                        let schemaVersionInt = binaryWriter.ReadInt64() |> int64FromBigEndian
                        $"admin/v2/schemas/%s{topicRestPath}/schema/%d{schemaVersionInt}"
                    | None ->
                        $"admin/v2/schemas/%s{topicRestPath}/schema"
                let url = randomServiceUri.AbsoluteUri + path
                let! schemaResponse =
                    pulsarHttpClient.Get<{| Version : Int64;
                                               Type: SchemaType;
                                               Timestamp: Int64;
                                               Data: string;
                                               Properties: Dictionary<string, string> |}>(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                let schemaData =
                    match schemaResponse.Type with
                    | SchemaType.KEY_VALUE -> schemaResponse.Data |> this.GetKeyValueSchemaBytes
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

            with Flatten ex ->
                match ex with
                //  When there is no topic related schema, pulsar http rest api will return a 404 exception
                //  In this case the request it's success, and we can return a None schema
                | :? HttpRequestException as ex when ex.StatusCode = Nullable HttpStatusCode.NotFound ->
                    Log.Logger.LogWarning(ex, "No schema found for topic {0} version {1}", topicName, schemaVersion)
                    return None
                | _ ->
                    let delay = Math.Min(backoff.Next(), remainingTimeMs)
                    if delay <= 0 then
                        raise (TimeoutException "Could not GetSchema within configured timeout.")
                    Log.Logger.LogWarning(ex, "GetSchema failed will retry in {0} ms", delay)
                    do! Async.Sleep delay
                    return! this.GetSchemaInner(topicName, schemaVersion, backoff, remainingTimeMs - delay)
        }

    //  Convert key/value schema json string to schema bytes[]
    member private this.GetKeyValueSchemaBytes(keyValueDataString: string) =
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

