namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Net.Http
open System.Net.Http.Headers
open System.Threading
open System.Threading.Tasks
open System.Text.Json
open Pulsar.Client.Common
open Microsoft.Extensions.Logging

[<CLIMutable>]
type ControlledFailoverResponse = {
    ServiceUrl: string
    TlsTrustCertsFilePath: string
    AuthPluginClassName: string
    AuthParamsString: string
}
// Example
// {
// "serviceUrl": "pulsar+ssl://target:6651",
// "tlsTrustCertsFilePath": "/security/ca.cert.pem",
// "authPluginClassName":"org.apache.pulsar.client.impl.auth.AuthenticationTls",
// "authParamsString": " \"tlsCertFile\": \"/security/client.cert.pem\"
//     \"tlsKeyFile\": \"/security/client-pk8.pem\" "
// }

type ControlledClusterFailover
    (
        providerUrl: string,
        checkInterval: TimeSpan,
        defaultServiceInfo: ServiceInfo,
        urlProviderHeader: IReadOnlyDictionary<string, string>
    ) =

    let jsonOptions = JsonSerializerOptions(JsonSerializerDefaults.Web)
    let mutable currentServiceInfo = defaultServiceInfo
    let cts = new CancellationTokenSource()

    let run (ctx: IServiceInfoProviderContext) =
        backgroundTask {
            // https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
            use httpClient = new HttpClient(new HttpClientHandler())
            httpClient.DefaultRequestHeaders.Accept.Add(MediaTypeWithQualityHeaderValue("application/json"))
            for header in urlProviderHeader do
                httpClient.DefaultRequestHeaders.Add(header.Key, header.Value)
            while not cts.IsCancellationRequested do
                try
                    do! Task.Delay(checkInterval, cts.Token)
                    use! response = httpClient.GetAsync(providerUrl, cts.Token)
                    if response.IsSuccessStatusCode then
                        use! stream = response.Content.ReadAsStreamAsync()
                        let! failoverResponse =
                            JsonSerializer.DeserializeAsync<ControlledFailoverResponse>(stream, jsonOptions)
                        let newServiceUrl = failoverResponse.ServiceUrl
                        // This is a minimal implementation of ControlledClusterFailover
                        if not (String.IsNullOrEmpty(newServiceUrl))
                           && newServiceUrl <> currentServiceInfo.ServiceUrl.OriginalString then
                            let newServiceInfo = ServiceInfo(newServiceUrl)
                            Log.Logger.LogInformation("ControlledClusterFailover switching to {0}", newServiceUrl)
                            currentServiceInfo <- newServiceInfo
                            do! ctx.UpdateServiceInfo(newServiceInfo)
                    else
                        Log.Logger.LogWarning("ControlledClusterFailover failed to fetch config from {0}, status {1}", providerUrl, response.StatusCode)
                with
                | :? OperationCanceledException when cts.IsCancellationRequested -> ()
                | :? TaskCanceledException when cts.IsCancellationRequested -> ()
                | Flatten ex ->
                    Log.Logger.LogError(ex, "Error checking controlled cluster failover url")
        }
        |> ignore

    interface IServiceInfoProvider with
        member this.Initialize(context: IServiceInfoProviderContext) =
            run context
        member this.GetServiceInfo() = currentServiceInfo
        member this.Dispose() =
            cts.Cancel()
            cts.Dispose()

type ControlledClusterFailoverBuilder() =
    let mutable providerUrl = ""
    let mutable checkInterval = TimeSpan.FromMinutes(1.0)
    let mutable defaultServiceInfo = None
    let mutable urlProviderHeader = readOnlyDict []

    member this.ProviderUrl(url: string) =
        providerUrl <- url
        this

    member this.CheckInterval(interval: TimeSpan) =
        checkInterval <- interval
        this

    member this.DefaultServiceInfo(serviceInfo: ServiceInfo) =
        defaultServiceInfo <- serviceInfo |> invalidArgIfDefault "ServiceInfo can't be null" |> Some
        this

    member this.UrlProviderHeader(header: IReadOnlyDictionary<string, string>) =
        urlProviderHeader <- header
        this

    member this.Build() : IServiceInfoProvider =
        if String.IsNullOrEmpty(providerUrl) then
            invalidArg "providerUrl" "providerUrl shouldn't be null or empty"
        if defaultServiceInfo.IsNone then
            invalidArg "defaultServiceInfo" "defaultServiceInfo must be set"
        if isNull urlProviderHeader then
            invalidArg "urlProviderHeader" "UrlProviderHeader shouldn't be null"

        new ControlledClusterFailover(
            providerUrl,
            checkInterval,
            defaultServiceInfo.Value,
            urlProviderHeader
        ) :> IServiceInfoProvider
