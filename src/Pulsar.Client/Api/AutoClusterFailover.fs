namespace Pulsar.Client.Api

open System
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open System.Timers
open Microsoft.Extensions.Logging
open Pulsar.Client.Common
open Pulsar.Client.Internal

type internal AutoServiceInfo = {
    ServiceInfo: ServiceInfo
    EndPointResolver: EndPointResolver
}

// Pure state machine

[<RequireQualifiedAccess>]
type internal AutoClusterMode =
    | Primary
    | Secondary of index: int

type internal AutoClusterState = {
    Mode: AutoClusterMode
    PrimaryFailedTimestamp: DateTime option
    PrimaryRecoveredTimestamp: DateTime option
}

[<RequireQualifiedAccess>]
type internal AutoClusterDecision =
    | NoAction
    | SwitchToSecondary of index: int
    | SwitchToPrimary

type internal AutoClusterConfig = {
    FailoverDelay: TimeSpan
    SwitchBackDelay: TimeSpan
    SecondaryCount: int
}

[<RequireQualifiedAccess>]
module internal AutoClusterFailoverLogic =

    let initialState = {
        Mode = AutoClusterMode.Primary
        PrimaryFailedTimestamp = None
        PrimaryRecoveredTimestamp = None
    }

    /// Single pure state transition covering both primary and secondary modes.
    /// - primaryAvailable: result of probing the primary endpoint.
    /// - findFirstAvailableSecondary: get index of the first available secondary
    ///   (None means no secondary was probed or none was available).
    let step
        (now: DateTime)
        (config: AutoClusterConfig)
        (primaryAvailable: bool)
        (findFirstAvailableSecondary: unit -> Task<int option>)
        (state: AutoClusterState) =
        backgroundTask {            
            match state.Mode, primaryAvailable with
            | AutoClusterMode.Primary, true ->
                return { state with PrimaryFailedTimestamp = None }, AutoClusterDecision.NoAction
            | AutoClusterMode.Primary, false ->
                match state.PrimaryFailedTimestamp with
                | None ->
                    return { state with PrimaryFailedTimestamp = Some now }, AutoClusterDecision.NoAction
                | Some ts when now - ts >= config.FailoverDelay ->
                    match! findFirstAvailableSecondary() with
                    | Some idx ->
                        return {
                            Mode = AutoClusterMode.Secondary idx
                            PrimaryFailedTimestamp = None
                            PrimaryRecoveredTimestamp = None
                        }, AutoClusterDecision.SwitchToSecondary idx
                    | None ->
                        Log.Logger.LogWarning("Secondary cluster is not available yet after failover delay")
                        return state, AutoClusterDecision.NoAction
                | _ ->
                    return state, AutoClusterDecision.NoAction
            | AutoClusterMode.Secondary _, true ->
                match state.PrimaryRecoveredTimestamp with
                | None ->
                    return { state with PrimaryRecoveredTimestamp = Some now }, AutoClusterDecision.NoAction
                | Some ts when now - ts >= config.SwitchBackDelay ->
                    return {
                        Mode = AutoClusterMode.Primary
                        PrimaryFailedTimestamp = None
                        PrimaryRecoveredTimestamp = None
                    }, AutoClusterDecision.SwitchToPrimary
                | _ ->
                    return state, AutoClusterDecision.NoAction
            | AutoClusterMode.Secondary _, false ->
                return { state with PrimaryRecoveredTimestamp = None }, AutoClusterDecision.NoAction
        }

// Orchestrator

type AutoClusterFailover
    internal
    (
        primary: ServiceInfo,
        secondary: ServiceInfo array,
        failoverDelay: TimeSpan,
        switchBackDelay: TimeSpan,
        checkInterval: TimeSpan,
        getCurrentTime: unit -> DateTime,
        probeAvailable: EndPointResolver -> Task<bool>,
        getTickScheduler: ((unit -> Task<unit>) -> IDisposable) option
    ) =

    let getAutoServiceInfo (serviceInfo: ServiceInfo) =
        { ServiceInfo = serviceInfo; EndPointResolver = EndPointResolver(serviceInfo.ServiceUrl.Addresses) }

    let config = {
        FailoverDelay = failoverDelay
        SwitchBackDelay = switchBackDelay
        SecondaryCount = secondary.Length
    }

    let primaryServiceInfo = getAutoServiceInfo primary
    let secondaryServiceInfos = secondary |> Array.map getAutoServiceInfo
    let mutable currentServiceInfo = primaryServiceInfo
    let mutable state = AutoClusterFailoverLogic.initialState

    let mutable context: IServiceInfoProviderContext option = None
    let mutable isDisposed = false

    let findFirstAvailableSecondary () =
        task {
            let mutable found = None
            let mutable i = 0
            while found.IsNone && i < secondaryServiceInfos.Length do
                let! avail = probeAvailable secondaryServiceInfos[i].EndPointResolver
                if avail then found <- Some i
                i <- i + 1
            return found
        }

    let applyDecision (decision: AutoClusterDecision) =
        backgroundTask {
            match decision with
            | AutoClusterDecision.SwitchToSecondary idx ->
                let sec = secondaryServiceInfos[idx]
                Log.Logger.LogInformation("Switching to secondary cluster {0}", sec.ServiceInfo.ServiceUrl)
                currentServiceInfo <- sec
                match context with
                | Some ctx -> do! ctx.UpdateServiceInfo(sec.ServiceInfo)
                | None -> ()
            | AutoClusterDecision.SwitchToPrimary ->
                Log.Logger.LogInformation("Switching back to primary cluster {0}", primary.ServiceUrl)
                currentServiceInfo <- primaryServiceInfo
                match context with
                | Some ctx -> do! ctx.UpdateServiceInfo(primary)
                | None -> ()
            | AutoClusterDecision.NoAction -> ()
        }

    let tick () =
        backgroundTask {
            try
                let! primaryAvailable = probeAvailable primaryServiceInfo.EndPointResolver
                let now = getCurrentTime()
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config primaryAvailable findFirstAvailableSecondary state
                state <- newState
                do! applyDecision decision
            with Flatten ex ->
                Log.Logger.LogError(ex, "Error checking cluster")
        }

    let timer =
        match getTickScheduler with
        | None ->
            let t = new Timer(checkInterval.TotalMilliseconds)
            t.AutoReset <- false
            t.Elapsed.Add(fun _ ->
                backgroundTask {
                    if not isDisposed then
                        do! tick()
                        try t.Start() with _ -> ()
                } |> ignore)
            t :> IDisposable
        | Some getScheduler ->
            getScheduler(tick)

    /// Production constructor — uses real clock, TCP probe, and timer-based scheduler.
    new(primary, secondary, failoverDelay, switchBackDelay, checkInterval) =
        let defaultProbe (resolver: EndPointResolver) =
            backgroundTask {
                let endpoint = resolver.Resolve()
                try
                    use client = new TcpClient()
                    use cts = new CancellationTokenSource(30_000)
                    do! client.ConnectAsync(endpoint.Host, endpoint.Port, cts.Token)
                    return true
                with Flatten ex ->
                    Log.Logger.LogWarning(ex, "Failed to probe available, url: {0}", endpoint)
                    return false
            }
        new AutoClusterFailover(
            primary, secondary, failoverDelay, switchBackDelay, checkInterval,
            (fun () -> DateTime.UtcNow),
            defaultProbe,
            None
        )

    interface IServiceInfoProvider with
        member _.Initialize(ctx: IServiceInfoProviderContext) =
            Log.Logger.LogInformation("Initializing AutoClusterFailover")
            context <- Some ctx
            match getTickScheduler with
            | None ->
                // Start the production timer on Initialize
                (timer :?> Timer).Start()
            | Some _ ->
                // Test scheduler is already ready; ticks are driven externally
                ()
        member _.GetServiceInfo() = currentServiceInfo.ServiceInfo

        member _.Dispose() =
            isDisposed <- true
            timer.Dispose()


type AutoClusterFailoverBuilder() =
    let mutable primary = None
    let mutable secondary = [||]
    let mutable failoverDelay = TimeSpan.FromSeconds(30.0)
    let mutable switchBackDelay = TimeSpan.FromSeconds(60.0)
    let mutable checkInterval = TimeSpan.FromSeconds(30.0)

    member this.Primary(serviceInfo: ServiceInfo) =
        primary <- serviceInfo |> invalidArgIfDefault "ServiceInfo can't be null" |> Some
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
            invalidArg "primary" "Primary serviceInfo must be set"
        if Array.isEmpty secondary then
            invalidArg "secondary" "Secondary serviceInfo list should have at least one item"
        
        new AutoClusterFailover(
            primary.Value,
            secondary,
            failoverDelay, 
            switchBackDelay, 
            checkInterval
        ) :> IServiceInfoProvider
