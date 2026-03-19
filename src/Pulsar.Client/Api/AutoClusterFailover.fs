namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open System.Security.Cryptography.X509Certificates
open Pulsar.Client.Common
open Pulsar.Client.Internal

type private ServiceInfo = {
    Url: string
    EndPointResolver: EndPointResolver
}

type AutoClusterFailover
    (
        primary: string,
        secondary: string array,
        primaryAuthentication: Authentication,
        secondaryAuthentication: IReadOnlyDictionary<string, Authentication>,
        primaryTlsTrustCertificate: X509Certificate2,
        secondaryTlsTrustCertificate: IReadOnlyDictionary<string, X509Certificate2>,
        failoverDelay: TimeSpan,
        switchBackDelay: TimeSpan,
        checkInterval: TimeSpan
    ) =

    let getServiceInfo (url: string) =
        let serviceUri =
            ServiceUri.parse(primary) |> Result.defaultWith (
                fun err -> failwith $"Invalid service url: %s{url}, error: %s{err}")
        { Url = url; EndPointResolver = EndPointResolver(serviceUri.Addresses) }

    let primaryServiceInfo = getServiceInfo primary
    let secondaryServiceInfos = secondary |> Array.map getServiceInfo
    let mutable currentServiceInfo = primaryServiceInfo
    let cts = new CancellationTokenSource()

    let mutable recoveredTimestamp: DateTime option = None
    let mutable failedTimestamp: DateTime option = None

    let probeAvailable (resolve: EndPointResolver) =
        backgroundTask {
            let endpoint = resolve.Resolve()
            try
                use client = new TcpClient()
                use cts = new CancellationTokenSource(30_000)
                do! client.ConnectAsync(endpoint.Host, endpoint.Port, cts.Token)
                return true
            with ex ->
                Log.Logger.LogWarning(ex, "Failed to probe available, url: {0}", endpoint)
                return false
        }

    let run (ctx: IServiceUrlProviderContext) =
        Log.Logger.LogInformation("Initializing AutoClusterFailover")
        backgroundTask {
            while not cts.IsCancellationRequested do
                try
                    do! Task.Delay checkInterval
                    if currentServiceInfo = primaryServiceInfo then
                        let! available = probeAvailable primaryServiceInfo.EndPointResolver
                        if not available then
                            match failedTimestamp with
                            | None ->
                                failedTimestamp <- Some DateTime.UtcNow
                            | Some ts when DateTime.UtcNow - ts >= failoverDelay ->
                                let! targetSecondary =
                                    task {
                                        let mutable found = None
                                        for sec in secondaryServiceInfos do
                                            if found.IsNone then
                                                let! avail = probeAvailable sec.EndPointResolver
                                                if avail then found <- Some sec
                                        return found
                                    }
                                match targetSecondary with
                                | Some sec ->
                                    Log.Logger.LogInformation("Switching to secondary cluster {0}", sec.Url)
                                    currentServiceInfo <- sec
                                    do! ctx.UpdateServiceUrl(sec.Url)
                                    if not (isNull secondaryAuthentication) && secondaryAuthentication.ContainsKey(sec.Url) then
                                        ctx.UpdateAuthentication(secondaryAuthentication[sec.Url])
                                    if not (isNull secondaryTlsTrustCertificate) && secondaryTlsTrustCertificate.ContainsKey(sec.Url) then
                                        ctx.UpdateTlsTrustCertificate(secondaryTlsTrustCertificate[sec.Url])
                                    failedTimestamp <- None
                                | None ->
                                    Log.Logger.LogWarning("Could not find any available secondary cluster")
                            | _ -> ()
                        else
                            failedTimestamp <- None
                    else
                        let! available = probeAvailable primaryServiceInfo.EndPointResolver
                        if available then
                            match recoveredTimestamp with
                            | None ->
                                recoveredTimestamp <- Some DateTime.UtcNow
                            | Some ts when DateTime.UtcNow - ts >= switchBackDelay ->
                                Log.Logger.LogInformation("Switching back to primary cluster {0}", primary)
                                currentServiceInfo <- primaryServiceInfo
                                do! ctx.UpdateServiceUrl(primary)
                                ctx.UpdateAuthentication(primaryAuthentication)
                                ctx.UpdateTlsTrustCertificate(primaryTlsTrustCertificate)
                                recoveredTimestamp <- None
                            | _ -> ()
                        else
                            recoveredTimestamp <- None
                with Flatten ex ->
                    Log.Logger.LogError(ex, "Error checking cluster")
        }
        |> ignore


    interface IServiceUrlProvider with
        member this.Initialize(context: IServiceUrlProviderContext) =
            run context
        member this.GetServiceUrl() = currentServiceInfo.Url

        member this.Dispose() =
            cts.Cancel()
            cts.Dispose()


type AutoClusterFailoverBuilder() =
    let mutable primary = ""
    let mutable secondary = [||]
    let mutable failoverDelay = TimeSpan.FromSeconds(30.0)
    let mutable switchBackDelay = TimeSpan.FromSeconds(60.0)
    let mutable checkInterval = TimeSpan.FromSeconds(30.0)
    let mutable primaryAuthentication = Authentication.AuthenticationDisabled
    let secondaryAuthentication = Dictionary<string, Authentication>()
    let mutable primaryTlsTrustCertificate = null : X509Certificate2
    let secondaryTlsTrustCertificate = Dictionary<string, X509Certificate2>()

    member this.Primary(url: string) =
        primary <- url
        this

    member this.Secondary(urls: string seq) =
        secondary <- urls |> Seq.toArray
        this

    member this.FailoverDelay(delay: TimeSpan) =
        failoverDelay <- delay
        this

    member this.SwitchBackDelay(delay: TimeSpan) =
        switchBackDelay <- delay
        this

    member this.CheckInterval(interval: TimeSpan) =
        checkInterval <- interval
        this

    member this.PrimaryAuthentication(authentication: Authentication) =
        primaryAuthentication <- authentication
        this

    member this.SecondaryAuthentication(secondaryAuth: IReadOnlyDictionary<string, Authentication>) =
        for kv in secondaryAuth do
            secondaryAuthentication[kv.Key] <- kv.Value
        this

    member this.PrimaryTlsTrustCertificate(certificate: X509Certificate2) =
        primaryTlsTrustCertificate <- certificate
        this

    member this.SecondaryTlsTrustCertificate(secondaryCert: IReadOnlyDictionary<string, X509Certificate2>) =
        for kv in secondaryCert do
            secondaryTlsTrustCertificate[kv.Key] <- kv.Value
        this

    member this.Build() : IServiceUrlProvider =
        if String.IsNullOrEmpty(primary) then
            invalidArg "primary" "primary service url shouldn't be null or empty"
        if Array.isEmpty secondary then
            invalidArg "secondary" "secondary cluster service url shouldn't be null and should have at least one url"
        
        new AutoClusterFailover(
            primary, 
            secondary,
            primaryAuthentication, 
            secondaryAuthentication, 
            primaryTlsTrustCertificate, 
            secondaryTlsTrustCertificate, 
            failoverDelay, 
            switchBackDelay, 
            checkInterval
        ) :> IServiceUrlProvider

