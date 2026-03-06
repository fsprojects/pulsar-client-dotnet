namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open System.Text.Json
open Pulsar.Client.Common

type ControlledClusterFailover
    (
        providerUrl: string,
        checkInterval: TimeSpan,
        defaultServiceUrl: string,
        defaultAuthentication: Authentication,
        defaultTlsTrustCertificate: System.Security.Cryptography.X509Certificates.X509Certificate2
    ) =

    let mutable currentServiceUrl = defaultServiceUrl
    let mutable currentProviderContext = None : IServiceUrlProviderContext option
    let cts = new CancellationTokenSource()

    let checkTask = 
        async {
            use httpClient = new HttpClient()
            while not cts.IsCancellationRequested do
                try
                    do! Async.Sleep (int checkInterval.TotalMilliseconds)
                    let! response = httpClient.GetAsync(providerUrl, cts.Token) |> Async.AwaitTask
                    if response.IsSuccessStatusCode then
                        let! content = response.Content.ReadAsStringAsync() |> Async.AwaitTask
                        if not (String.IsNullOrEmpty(content)) then
                            // expects json: { "serviceUrl": "...", "authentication": "..." }
                            // For simplicity, we just look for serviceUrl for now. In Java, it parses Map<String,String> and uses auth plugins.
                            let json = JsonDocument.Parse(content)
                            let root = json.RootElement
                            let serviceUrlProp = 
                                match root.TryGetProperty("serviceUrl") with
                                | true, prop -> Some (prop.GetString())
                                | _ -> None
                                
                            // This is a minimal implementation of ControlledClusterFailover
                            match serviceUrlProp with
                            | Some newServiceUrl when not (String.IsNullOrEmpty(newServiceUrl)) && newServiceUrl <> currentServiceUrl ->
                                Log.Logger.LogInformation("ControlledClusterFailover switching to {0}", newServiceUrl)
                                currentServiceUrl <- newServiceUrl
                                match currentProviderContext with
                                | Some ctx -> ctx.UpdateServiceUrl(newServiceUrl)
                                | None -> ()
                            | _ -> ()
                    else
                        Log.Logger.LogWarning("ControlledClusterFailover failed to fetch config from {0}, status {1}", providerUrl, response.StatusCode)
                with
                | :? TaskCanceledException -> ()
                | :? OperationCanceledException -> ()
                | ex ->
                    Log.Logger.LogError(ex, "Error checking controlled cluster failover url")
        }

    let checkTaskHandle = Async.StartAsTask(checkTask, cancellationToken = cts.Token)

    interface IServiceUrlProvider with
        member this.Initialize(context: IServiceUrlProviderContext) =
            currentProviderContext <- Some context
        member this.ServiceUrl = currentServiceUrl
        member this.Dispose() =
            cts.Cancel()
            cts.Dispose()

type ControlledClusterFailoverBuilder() =
    let mutable providerUrl = ""
    let mutable checkInterval = TimeSpan.FromMinutes(1.0)
    let mutable defaultServiceUrl = ""
    let mutable defaultAuthentication = Authentication.AuthenticationDisabled
    let mutable defaultTlsTrustCertificate = null

    member this.ProviderUrl(url: string) =
        providerUrl <- url
        this

    member this.CheckInterval(interval: TimeSpan) =
        checkInterval <- interval
        this

    member this.DefaultServiceUrl(url: string) =
        defaultServiceUrl <- url
        this

    member this.DefaultAuthentication(authentication: Authentication) =
        defaultAuthentication <- authentication
        this

    member this.DefaultTlsTrustCertificate(certificate: System.Security.Cryptography.X509Certificates.X509Certificate2) =
        defaultTlsTrustCertificate <- certificate
        this

    member this.Build() : IServiceUrlProvider =
        if String.IsNullOrEmpty(providerUrl) then
            invalidArg "providerUrl" "providerUrl shouldn't be null or empty"
        if String.IsNullOrEmpty(defaultServiceUrl) then
            invalidArg "defaultServiceUrl" "defaultServiceUrl shouldn't be null or empty"

        new ControlledClusterFailover(
            providerUrl,
            checkInterval,
            defaultServiceUrl,
            defaultAuthentication,
            defaultTlsTrustCertificate
        ) :> IServiceUrlProvider
