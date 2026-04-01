namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Http
open System.Text.Json
open System.Text.Json.Serialization
open System.Threading
open FSharp.UMX
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Schema

//  This class is mainly used for http lookup service
//  We name this class `PulsarHttpClient` to avoid naming clash with native HttpClient, and in Java pulsar client it's just `HttpClient`
type internal PulsarHttpClient() =

    let httpClient = new HttpClient(new HttpClientHandler(AllowAutoRedirect = true))
    let jsonOptions =
        JsonSerializerOptions(
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        )

    do jsonOptions.Converters.Add(JsonStringEnumConverter())

    member _.Get<'T>(requestUri: string, auth: Authentication) =
        backgroundTask {
            let authenticationDataProvider = auth.GetAuthData()
            if authenticationDataProvider.HasDataForHttp() then
                use request = new HttpRequestMessage(HttpMethod.Get, requestUri)
                for headerPropertyEntry in authenticationDataProvider.GetHttpHeaders() do
                    request.Headers.Add(headerPropertyEntry.Key, headerPropertyEntry.Value)
                use! response = httpClient.SendAsync(request)
                response.EnsureSuccessStatusCode() |> ignore
                use! stream = response.Content.ReadAsStreamAsync()
                return JsonSerializer.Deserialize<'T>(stream, jsonOptions)
            else
                use! response = httpClient.GetAsync(requestUri)
                response.EnsureSuccessStatusCode() |> ignore
                use! stream = response.Content.ReadAsStreamAsync()
                return JsonSerializer.Deserialize<'T>(stream, jsonOptions)
        }

    member _.GetResponse(requestUri: string, auth: Authentication) =
        backgroundTask {
            let authenticationDataProvider = auth.GetAuthData()
            if authenticationDataProvider.HasDataForHttp() then
                let request = new HttpRequestMessage(HttpMethod.Get, requestUri)
                for headerPropertyEntry in authenticationDataProvider.GetHttpHeaders() do
                    request.Headers.Add(headerPropertyEntry.Key, headerPropertyEntry.Value)
                let! response = httpClient.SendAsync(request)
                return request, response
            else
                let! response = httpClient.GetAsync(requestUri)
                return null, response
        }

    member _.Dispose() =
        httpClient.Dispose()


type internal HttpLookupService(config: PulsarClientConfiguration) =

    let pulsarHttpClient = PulsarHttpClient()
    let jsonOptions =
        JsonSerializerOptions(
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        )
    let mutable currentServiceInfo =
        ServiceInfo(
            {
                OriginalString = "" // not used here
                Addresses = config.ServiceAddresses
                UseTls = config.UseTls
                Scheme = config.Scheme
            },
            config.Authentication,
            config.TlsTrustCertificate
        )

    do jsonOptions.Converters.Add(JsonStringEnumConverter())

    let getCurrentServiceInfo() = Volatile.Read(&currentServiceInfo)

    interface ILookupService with

        member this.GetPartitionsForTopic(topicName: TopicName) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                let! metadata =
                    this.GetPartitionedTopicMetadataInner(
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
                return!
                    this.GetPartitionedTopicMetadataInner(
                        topicName,
                        backoff,
                        int config.OperationTimeout.TotalMilliseconds
                    )
            }

        member _.GetBroker(topicName: CompleteTopicName) =
            backgroundTask {
                let serviceInfo = getCurrentServiceInfo()
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://", "persistent/").Replace("non-persistent://", "non-persistent/")
                let url = randomServiceUri.AbsoluteUri + $"lookup/v2/topic/%s{topicRestPath}"
                let! brokerResponse =
                    pulsarHttpClient.Get<{| BrokerUrl: string; BrokerUrlTls: string; HttpUrl: string; HttpUrlTls: string |}>(
                        url,
                        serviceInfo.Authentication
                    )
                let uri =
                    if serviceInfo.ServiceUrl.UseTls then
                        Uri(brokerResponse.BrokerUrlTls)
                    else
                        Uri(brokerResponse.BrokerUrl)
                let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
                return { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
            }

        member this.GetTopicsUnderNamespace(ns: NamespaceName, isPersistent: bool) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                return!
                    this.GetTopicsUnderNamespaceInner(
                        ns,
                        backoff,
                        int config.OperationTimeout.TotalMilliseconds,
                        isPersistent
                    )
            }

        member this.GetSchema(topicName: CompleteTopicName, ?schemaVersion: SchemaVersion) =
            backgroundTask {
                let backoff =
                    Backoff {
                        Initial = TimeSpan.FromSeconds(5.0)
                        MandatoryStop = config.OperationTimeout + config.OperationTimeout
                        Max = TimeSpan.FromMinutes(1.0)
                    }
                return!
                    this.GetSchemaInner(
                        topicName,
                        schemaVersion,
                        backoff,
                        int config.OperationTimeout.TotalMilliseconds
                    )
            }

        member _.UpdateServiceInfo(serviceInfo: ServiceInfo) =
            Volatile.Write(&currentServiceInfo, serviceInfo)

        member _.Dispose() =
            pulsarHttpClient.Dispose()

    //  GET /admin/v2/{topic-domain}/{tenant}/{namespace}/{topic}/partitions?checkAllowAutoCreation=true
    member private this.GetPartitionedTopicMetadataInner(topicName: CompleteTopicName, backoff: Backoff, remainingTimeMs) =
        async {
            try
                let serviceInfo = getCurrentServiceInfo()
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://", "persistent/").Replace("non-persistent://", "non-persistent/")
                let url = randomServiceUri.AbsoluteUri + $"admin/v2/%s{topicRestPath}/partitions?checkAllowAutoCreation=true"
                let! metadataResponse =
                    pulsarHttpClient.Get<{| Partitions: int |}>(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                return { Partitions = metadataResponse.Partitions }
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
    member private this.GetTopicsUnderNamespaceInner(ns: NamespaceName, backoff: Backoff, remainingTimeMs: int, isPersistent: bool) =
        async {
            try
                let serviceInfo = getCurrentServiceInfo()
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let mode =
                    match isPersistent with
                    | true -> "PERSISTENT"
                    | false -> "NON_PERSISTENT"
                let url = randomServiceUri.AbsoluteUri + $"admin/v2/namespaces/%s{ns.ToString()}/topics?mode=%s{mode}"
                let! topics =
                    pulsarHttpClient.Get<string[]>(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                return topics :> string seq
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
    member private this.GetSchemaInner(topicName: CompleteTopicName, schemaVersion: SchemaVersion option, backoff: Backoff, remainingTimeMs: int) =
        async {
            try
                let serviceInfo = getCurrentServiceInfo()
                let addresses = serviceInfo.ServiceUrl.Addresses
                let randomServiceUri = addresses[RandomGenerator.Next(0, addresses.Length)]
                let topic: string = %topicName
                let topicRestPath = topic.Replace("persistent://", "").Replace("non-persistent://", "")
                let path =
                    match schemaVersion with
                    | Some sv ->
                        use binaryReader = new BinaryReader(new MemoryStream(sv.Bytes))
                        let schemaVersionInt = binaryReader.ReadInt64() |> int64FromBigEndian
                        $"admin/v2/schemas/%s{topicRestPath}/schema/%d{schemaVersionInt}"
                    | None ->
                        $"admin/v2/schemas/%s{topicRestPath}/schema"
                let url = randomServiceUri.AbsoluteUri + path
                let! request, responseMessage =
                    pulsarHttpClient.GetResponse(url, serviceInfo.Authentication)
                    |> Async.AwaitTask
                use request = request
                use responseMessage = responseMessage
                if responseMessage.StatusCode = HttpStatusCode.NotFound then
                    Log.Logger.LogWarning("No schema found for topic {0} version {1}", topicName, schemaVersion)
                    return None
                else
                    responseMessage.EnsureSuccessStatusCode() |> ignore
                    use! response = responseMessage.Content.ReadAsStreamAsync() |> Async.AwaitTask
                    let schemaResponse =
                        JsonSerializer.Deserialize<{| Version: Int64; Type: SchemaType; Timestamp: Int64; Data: string; Properties: Dictionary<string, string> |}>(
                            response,
                            jsonOptions
                        )
                    let schemaData =
                        match schemaResponse.Type with
                        | SchemaType.KEY_VALUE -> schemaResponse.Data |> this.GetKeyValueSchemaBytes
                        | _ -> schemaResponse.Data |> System.Text.Encoding.UTF8.GetBytes
                    let resolvedSchemaVersion: SchemaVersion =
                        {
                            Bytes = schemaResponse.Version |> BitConverter.GetBytes |> Array.rev
                        }
                    let schemaInfo: SchemaInfo =
                        {
                            Name = topicRestPath
                            Type = schemaResponse.Type
                            Properties = schemaResponse.Properties
                            Schema = schemaData
                        }
                    let topicSchema: TopicSchema =
                        {
                            SchemaVersion = Some resolvedSchemaVersion
                            SchemaInfo = schemaInfo
                        }
                    return Some topicSchema
            with Flatten ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not GetSchema within configured timeout.")
                Log.Logger.LogWarning(ex, "GetSchema failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetSchemaInner(topicName, schemaVersion, backoff, remainingTimeMs - delay)
        }

    //  Convert key/value schema json string to schema bytes[]
    member private _.GetKeyValueSchemaBytes(keyValueDataString: string) =
        //  Key-value type schema `Data` field is a complicated embedded json string
        //  so that it's difficult to deserialize the field by orm method.
        //  Instead, we only need to extract 'key' and 'value' field and call `KeyValueSchema.GetKeyValueBytes` method.
        use json = JsonDocument.Parse(keyValueDataString)
        let keyString = json.RootElement.GetProperty("key").GetRawText()
        let valueString = json.RootElement.GetProperty("value").GetRawText()
        KeyValueSchema.GetKeyValueBytes(
            keyString |> System.Text.Encoding.UTF8.GetBytes,
            valueString |> System.Text.Encoding.UTF8.GetBytes
        )
