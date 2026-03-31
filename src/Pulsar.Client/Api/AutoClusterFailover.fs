namespace Pulsar.Client.Api

open System
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Pulsar.Client.Common
open Pulsar.Client.Internal

type private AutoServiceInfo = {
    ServiceInfo: ServiceInfo
    EndPointResolver: EndPointResolver
}

type AutoClusterFailover
    (
        primary: ServiceInfo,
        secondary: ServiceInfo array,
        failoverDelay: TimeSpan,
        switchBackDelay: TimeSpan,
        checkInterval: TimeSpan
    ) =

    let getAutoServiceInfo (serviceInfo: ServiceInfo) =
        { ServiceInfo = serviceInfo; EndPointResolver = EndPointResolver(serviceInfo.ServiceUrl.Addresses) }

    let primaryServiceInfo = getAutoServiceInfo primary
    let secondaryServiceInfos = secondary |> Array.map getAutoServiceInfo
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

    let run (ctx: IServiceInfoProviderContext) =
        Log.Logger.LogInformation("Initializing AutoClusterFailover")
        backgroundTask {
            while not cts.IsCancellationRequested do
                try
                    do! Task.Delay(checkInterval, cts.Token)
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
                                    Log.Logger.LogInformation("Switching to secondary cluster {0}", sec.ServiceInfo.ServiceUrl)
                                    currentServiceInfo <- sec
                                    do! ctx.UpdateServiceInfo(sec.ServiceInfo)
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
                                do! ctx.UpdateServiceInfo(primary)
                                recoveredTimestamp <- None
                            | _ -> ()
                        else
                            recoveredTimestamp <- None
                with Flatten ex ->
                    Log.Logger.LogError(ex, "Error checking cluster")
        }
        |> ignore


    interface IServiceInfoProvider with
        member this.Initialize(context: IServiceInfoProviderContext) =
            run context
        member this.GetServiceInfo() = currentServiceInfo.ServiceInfo

        member this.Dispose() =
            cts.Cancel()
            cts.Dispose()


type AutoClusterFailoverBuilder() =
    let mutable primary = None
    let mutable secondary = [||]
    let mutable failoverDelay = TimeSpan.FromSeconds(30.0)
    let mutable switchBackDelay = TimeSpan.FromSeconds(60.0)
    let mutable checkInterval = TimeSpan.FromSeconds(30.0)

    member this.Primary(serviceInfo: ServiceInfo) =
        primary <- Some serviceInfo
        this

    member this.Secondary(serviceInfos: ServiceInfo seq) =
        secondary <- serviceInfos |> Seq.toArray
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

    member this.Build() : IServiceInfoProvider =
        if primary.IsNone then
            invalidArg "primary" "Primary serviceInfo shouldn't be null or empty"
        if Array.isEmpty secondary then
            invalidArg "secondary" "Secondary serviceInfo list should have at least one item"
        
        new AutoClusterFailover(
            primary.Value,
            secondary,
            failoverDelay, 
            switchBackDelay, 
            checkInterval
        ) :> IServiceInfoProvider

