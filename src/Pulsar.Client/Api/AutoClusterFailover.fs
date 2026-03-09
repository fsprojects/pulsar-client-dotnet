namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Pulsar.Client.Common

type FailoverPolicy =
    | Order = 0

type AutoClusterFailover
    (
        primary: string,
        secondary: string list,
        failoverPolicy: FailoverPolicy,
        primaryAuthentication: Authentication,
        secondaryAuthentication: IReadOnlyDictionary<string, Authentication>,
        primaryTlsTrustCertificate: System.Security.Cryptography.X509Certificates.X509Certificate2,
        secondaryTlsTrustCertificate: IReadOnlyDictionary<string, System.Security.Cryptography.X509Certificates.X509Certificate2>,
        failoverDelay: TimeSpan,
        switchBackDelay: TimeSpan,
        checkInterval: TimeSpan
    ) =

    let mutable currentServiceUrl = primary
    let mutable currentProviderContext = None : IServiceUrlProviderContext option
    let cts = new CancellationTokenSource()

    let mutable recoveredTimestamp = 0L
    let mutable failedTimestamp = 0L

    let probeAvailable (url: string) =
        async {
            try
                let uri = Uri(url.Replace("pulsar+ssl://", "pulsar://").Replace("http://", "pulsar://").Replace("https://", "pulsar://")) // ensure Uri parses it correctly just for host/port extraction
                use client = new TcpClient()
                let connectTask = client.ConnectAsync(uri.Host, uri.Port)
                let! completedTask = Task.WhenAny(connectTask, Task.Delay(1000)) |> Async.AwaitTask
                if completedTask = (connectTask :> Task) && connectTask.Status = TaskStatus.RanToCompletion then
                    return true
                else
                    return false
            with ex ->
                Log.Logger.LogWarning(ex, "Failed to probe available, url: {0}", url)
                return false
        }

    let checkTask = 
        async {
            while not cts.IsCancellationRequested do
                try
                    do! Async.Sleep (int checkInterval.TotalMilliseconds)
                    if currentServiceUrl = primary then
                        let! available = probeAvailable primary
                        if not available then
                            if failedTimestamp = 0L then
                                failedTimestamp <- DateTime.UtcNow.Ticks
                            elif TimeSpan.FromTicks(DateTime.UtcNow.Ticks - failedTimestamp) >= failoverDelay then
                                let! targetSecondary = 
                                    async {
                                        let mutable found = None
                                        for sec in secondary do
                                            if found.IsNone then
                                                let! avail = probeAvailable sec
                                                if avail then found <- Some sec
                                        return found
                                    }
                                match targetSecondary with
                                | Some sec ->
                                    Log.Logger.LogInformation("Switching to secondary cluster {0}", sec)
                                    currentServiceUrl <- sec
                                    match currentProviderContext with
                                    | Some ctx ->
                                        ctx.UpdateServiceUrl(sec)
                                        if not (box secondaryAuthentication = null) && secondaryAuthentication.ContainsKey(sec) then
                                            ctx.UpdateAuthentication(secondaryAuthentication.[sec])
                                        if not (box secondaryTlsTrustCertificate = null) && secondaryTlsTrustCertificate.ContainsKey(sec) then
                                            ctx.UpdateTlsTrustCertificate(secondaryTlsTrustCertificate.[sec])
                                    | None -> ()
                                    failedTimestamp <- 0L
                                | None -> ()
                        else
                            failedTimestamp <- 0L
                    else
                        let! available = probeAvailable primary
                        if available then
                            if recoveredTimestamp = 0L then
                                recoveredTimestamp <- DateTime.UtcNow.Ticks
                            elif TimeSpan.FromTicks(DateTime.UtcNow.Ticks - recoveredTimestamp) >= switchBackDelay then
                                Log.Logger.LogInformation("Switching back to primary cluster {0}", primary)
                                currentServiceUrl <- primary
                                match currentProviderContext with
                                | Some ctx ->
                                    ctx.UpdateServiceUrl(primary)
                                    ctx.UpdateAuthentication(primaryAuthentication)
                                    ctx.UpdateTlsTrustCertificate(primaryTlsTrustCertificate)
                                | None -> ()
                                recoveredTimestamp <- 0L
                        else
                            recoveredTimestamp <- 0L
                with
                | :? TaskCanceledException -> ()
                | :? OperationCanceledException -> ()
                | ex ->
                    Log.Logger.LogError(ex, "Error checking cluster")
        }

    let checkTaskHandle = Async.StartAsTask(checkTask, cancellationToken = cts.Token)

    interface IServiceUrlProvider with
        member this.Initialize(context: IServiceUrlProviderContext) =
            currentProviderContext <- Some context
        member this.GetServiceUrl() = currentServiceUrl
        member this.Dispose() =
            cts.Cancel()
            cts.Dispose()


type AutoClusterFailoverBuilder() =
    let mutable primary = ""
    let mutable secondary = []
    let mutable failoverDelay = TimeSpan.FromSeconds(30.0)
    let mutable switchBackDelay = TimeSpan.FromSeconds(60.0)
    let mutable checkInterval = TimeSpan.FromSeconds(30.0)
    let mutable failoverPolicy = FailoverPolicy.Order
    let mutable primaryAuthentication = Authentication.AuthenticationDisabled
    let secondaryAuthentication = Dictionary<string, Authentication>()
    let mutable primaryTlsTrustCertificate = null : System.Security.Cryptography.X509Certificates.X509Certificate2
    let secondaryTlsTrustCertificate = Dictionary<string, System.Security.Cryptography.X509Certificates.X509Certificate2>()

    member this.Primary(url: string) =
        primary <- url
        this

    member this.Secondary(urls: string seq) =
        secondary <- urls |> Seq.toList
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
 
    member this.FailoverPolicy(policy: FailoverPolicy) =
        failoverPolicy <- policy
        this

    member this.PrimaryAuthentication(authentication: Authentication) =
        primaryAuthentication <- authentication
        this

    member this.SecondaryAuthentication(secondaryAuth: IReadOnlyDictionary<string, Authentication>) =
        for kv in secondaryAuth do
            secondaryAuthentication[kv.Key] <- kv.Value
        this

    member this.PrimaryTlsTrustCertificate(certificate: System.Security.Cryptography.X509Certificates.X509Certificate2) =
        primaryTlsTrustCertificate <- certificate
        this

    member this.SecondaryTlsTrustCertificate(secondaryCert: IReadOnlyDictionary<string, System.Security.Cryptography.X509Certificates.X509Certificate2>) =
        for kv in secondaryCert do
            secondaryTlsTrustCertificate[kv.Key] <- kv.Value
        this

    member this.Build() : IServiceUrlProvider =
        if String.IsNullOrEmpty(primary) then
            invalidArg "primary" "primary service url shouldn't be null or empty"
        if List.isEmpty secondary then
            invalidArg "secondary" "secondary cluster service url shouldn't be null and should have at least one url"
        
        new AutoClusterFailover(
            primary, 
            secondary, 
            failoverPolicy, 
            primaryAuthentication, 
            secondaryAuthentication, 
            primaryTlsTrustCertificate, 
            secondaryTlsTrustCertificate, 
            failoverDelay, 
            switchBackDelay, 
            checkInterval
        ) :> IServiceUrlProvider

