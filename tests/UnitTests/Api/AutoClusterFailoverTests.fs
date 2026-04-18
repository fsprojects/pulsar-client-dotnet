namespace Pulsar.Client.UnitTests.Api

open System
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests

module AutoClusterFailoverTests =

    // ---- Helpers for pure logic tests ----

    let private t0 = DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc)
    let private config = {
        FailoverDelay = TimeSpan.FromSeconds(30.0)
        SwitchBackDelay = TimeSpan.FromSeconds(60.0)
        SecondaryCount = 2
    }
    let noSecondaryAvailable = fun () -> Task.FromResult(None)
    let secondaryAvailable index = fun () -> Task.FromResult(Some index)

    // ---- Pure state-machine tests ----

    [<Tests>]
    let stepTests =

        testList "AutoClusterFailoverLogic" [

            // -- step: on primary --

            testTask "step: primary available clears failedTimestamp" {
                let state = { AutoClusterFailoverLogic.initialState with PrimaryFailedTimestamp = Some t0 }
                let! newState, decision =
                    AutoClusterFailoverLogic.step t0 config true noSecondaryAvailable state
                newState.PrimaryFailedTimestamp |> Expect.isNone "FailedTimestamp should be cleared"
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: primary unavailable records failedTimestamp on first failure" {
                let state = AutoClusterFailoverLogic.initialState
                let! newState, decision =
                    AutoClusterFailoverLogic.step t0 config false noSecondaryAvailable state
                newState.PrimaryFailedTimestamp |> Expect.equal "should record timestamp" (Some t0)
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: does not switch before failover delay elapses" {
                let state = { AutoClusterFailoverLogic.initialState with PrimaryFailedTimestamp = Some t0 }
                let now = t0 + TimeSpan.FromSeconds(29.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config false (secondaryAvailable 0) state
                newState.Mode |> Expect.equal "should stay Primary" AutoClusterMode.Primary
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: switches to secondary after failover delay" {
                let state = { AutoClusterFailoverLogic.initialState with PrimaryFailedTimestamp = Some t0 }
                let now = t0 + TimeSpan.FromSeconds(30.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config false (secondaryAvailable 1) state
                newState.Mode |> Expect.equal "should switch to Secondary 1" (AutoClusterMode.Secondary 1)
                newState.PrimaryFailedTimestamp |> Expect.isNone "FailedTimestamp should be cleared"
                decision |> Expect.equal "should switch" (AutoClusterDecision.SwitchToSecondary 1)
            }

            testTask "step: no available secondary keeps state unchanged" {
                let state = { AutoClusterFailoverLogic.initialState with PrimaryFailedTimestamp = Some t0 }
                let now = t0 + TimeSpan.FromSeconds(30.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config false noSecondaryAvailable state
                newState.PrimaryFailedTimestamp |> Expect.equal "should keep timestamp" (Some t0)
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: primary becomes available resets failedTimestamp" {
                let state = { AutoClusterFailoverLogic.initialState with PrimaryFailedTimestamp = Some t0 }
                let now = t0 + TimeSpan.FromSeconds(10.0)
                let! newState, _ =
                    AutoClusterFailoverLogic.step now config true noSecondaryAvailable state
                newState.PrimaryFailedTimestamp |> Expect.isNone "FailedTimestamp should be cleared"
            }

            // -- step: on secondary --

            testTask "step: on secondary, primary available records recoveredTimestamp" {
                let state = { AutoClusterFailoverLogic.initialState with Mode = AutoClusterMode.Secondary 0 }
                let! newState, decision =
                    AutoClusterFailoverLogic.step t0 config true noSecondaryAvailable state
                newState.PrimaryRecoveredTimestamp |> Expect.equal "should record timestamp" (Some t0)
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: on secondary, does not switch back before switchBackDelay" {
                let state = {
                    Mode = AutoClusterMode.Secondary 0
                    PrimaryFailedTimestamp = None
                    PrimaryRecoveredTimestamp = Some t0
                }
                let now = t0 + TimeSpan.FromSeconds(59.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config true noSecondaryAvailable state
                newState.Mode |> Expect.equal "should stay Secondary" (AutoClusterMode.Secondary 0)
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }

            testTask "step: on secondary, switches back to primary after switchBackDelay" {
                let state = {
                    Mode = AutoClusterMode.Secondary 0
                    PrimaryFailedTimestamp = None
                    PrimaryRecoveredTimestamp = Some t0
                }
                let now = t0 + TimeSpan.FromSeconds(60.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config true noSecondaryAvailable state
                newState.Mode |> Expect.equal "should be Primary" AutoClusterMode.Primary
                newState.PrimaryRecoveredTimestamp |> Expect.isNone "RecoveredTimestamp should be cleared"
                decision |> Expect.equal "should switch back" AutoClusterDecision.SwitchToPrimary
            }

            testTask "step: on secondary, primary goes down again clears recoveredTimestamp" {
                let state = {
                    Mode = AutoClusterMode.Secondary 0
                    PrimaryFailedTimestamp = None
                    PrimaryRecoveredTimestamp = Some t0
                }
                let now = t0 + TimeSpan.FromSeconds(10.0)
                let! newState, decision =
                    AutoClusterFailoverLogic.step now config false noSecondaryAvailable state
                newState.PrimaryRecoveredTimestamp |> Expect.isNone "RecoveredTimestamp should be cleared"
                decision |> Expect.equal "should be Noop" AutoClusterDecision.NoAction
            }
        ]

    // ---- Orchestrator tests with fakes ----

    let private primaryUrl = "pulsar://primary.example.com:6650"
    let private secondary1Url = "pulsar://secondary1.example.com:6650"
    let private secondary2Url = "pulsar://secondary2.example.com:6650"
    let private delay30s = TimeSpan.FromSeconds(30.0)
    let private delay60s = TimeSpan.FromSeconds(60.0)

    /// Creates a fake probe function that returns availability based on a mutable map keyed by host name.
    let private createFakeProbe (availabilityByHost: System.Collections.Generic.Dictionary<string, bool>) =
        fun (resolver: EndPointResolver) ->
            let ep = resolver.Resolve()
            let available =
                match availabilityByHost.TryGetValue(ep.Host) with
                | true, v -> v
                | false, _ -> false
            Task.FromResult(available)

    /// Creates a fake context that records UpdateServiceInfo calls.
    let private createFakeContext () =
        let updates = System.Collections.Generic.List<ServiceInfo>()
        let ctx =
            { new IServiceInfoProviderContext with
                member _.UpdateServiceInfo(si) =
                    updates.Add(si)
                    Task.FromResult(()) }
        ctx, updates

    [<Tests>]
    let orchestratorTests =

        testList "AutoClusterFailover orchestrator" [

            test "GetServiceInfo returns primary before any tick" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- true

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> t0),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, _ = createFakeContext()
                provider.Initialize(ctx)

                provider.GetServiceInfo().ServiceUrl.OriginalString
                |> Expect.equal "should be primary" primaryUrl

                provider.Dispose()
            }

            test "Builder Build throws if primary is missing" {
                fun () -> AutoClusterFailoverBuilder().Secondary([| ServiceInfo(secondary1Url) |]).Build() |> ignore
                |> Expect.throwsWithMessage<ArgumentException> "Primary serviceInfo must be set (Parameter 'primary')"
            }

            test "Builder Build throws if secondary list is empty" {
                fun () -> AutoClusterFailoverBuilder().Primary(ServiceInfo(primaryUrl)).Build() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "Secondary serviceInfo list should have at least one item (Parameter 'secondary')"
            }

            testTask "Ticks switch to secondary when primary is down and failover delay passes" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- true
                availability["secondary1.example.com"] <- true

                let mutable now = t0

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> now),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, updates = createFakeContext()
                provider.Initialize(ctx)

                // Primary goes down
                availability["primary.example.com"] <- false

                // Tick 1: should record failure timestamp
                do! scheduler.Tick()
                updates.Count |> Expect.equal "no switch yet" 0
                provider.GetServiceInfo().ServiceUrl.OriginalString
                |> Expect.equal "still primary" primaryUrl

                // Advance time past failover delay
                now <- t0 + TimeSpan.FromSeconds(31.0)

                // Tick 2: failover delay elapsed, should switch
                do! scheduler.Tick()
                updates.Count |> Expect.equal "one switch" 1
                updates[0].ServiceUrl.OriginalString
                |> Expect.equal "switched to secondary" secondary1Url
                provider.GetServiceInfo().ServiceUrl.OriginalString
                |> Expect.equal "now secondary" secondary1Url

                provider.Dispose()
            }

            testTask "Ticks switch back to primary after recovery and switchBackDelay" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- false
                availability["secondary1.example.com"] <- true

                let mutable now = t0

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> now),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, updates = createFakeContext()
                provider.Initialize(ctx)

                // Tick 1: primary down, records failure
                do! scheduler.Tick()
                // Tick 2: advance past failoverDelay, switch to secondary
                now <- t0 + TimeSpan.FromSeconds(31.0)
                do! scheduler.Tick()
                updates.Count |> Expect.equal "failover happened" 1

                // Now simulate primary recovery
                availability["primary.example.com"] <- true
                let switchBackStart = now

                // Tick 3: records recovered timestamp
                do! scheduler.Tick()
                updates.Count |> Expect.equal "no switch back yet" 1

                // Tick 4: still within switchBackDelay
                now <- switchBackStart + TimeSpan.FromSeconds(59.0)
                do! scheduler.Tick()
                updates.Count |> Expect.equal "still no switch back" 1

                // Tick 5: past switchBackDelay, should switch back
                now <- switchBackStart + TimeSpan.FromSeconds(61.0)
                do! scheduler.Tick()
                updates.Count |> Expect.equal "switch back happened" 2
                updates[1].ServiceUrl.OriginalString
                |> Expect.equal "switched back to primary" primaryUrl
                provider.GetServiceInfo().ServiceUrl.OriginalString
                |> Expect.equal "now primary" primaryUrl

                provider.Dispose()
            }

            testTask "Probes secondaries in order and picks first available" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- false
                availability["secondary1.example.com"] <- false
                availability["secondary2.example.com"] <- true

                let mutable now = t0

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url); ServiceInfo(secondary2Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> now),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, updates = createFakeContext()
                provider.Initialize(ctx)

                // Tick 1: record failure
                do! scheduler.Tick()
                // Tick 2: past delay, secondary1 down, should pick secondary2
                now <- t0 + TimeSpan.FromSeconds(31.0)
                do! scheduler.Tick()

                updates.Count |> Expect.equal "one switch" 1
                updates[0].ServiceUrl.OriginalString
                |> Expect.equal "picked secondary2" secondary2Url

                provider.Dispose()
            }

            testTask "No switch when all secondaries are down" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- false
                availability["secondary1.example.com"] <- false

                let mutable now = t0

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> now),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, updates = createFakeContext()
                provider.Initialize(ctx)

                // Tick 1: record failure
                do! scheduler.Tick()
                // Tick 2: past delay, but no secondary available
                now <- t0 + TimeSpan.FromSeconds(31.0)
                do! scheduler.Tick()

                updates.Count |> Expect.equal "no switch" 0
                provider.GetServiceInfo().ServiceUrl.OriginalString
                |> Expect.equal "still primary" primaryUrl

                provider.Dispose()
            }

            testTask "Primary recovery resets failedTimestamp so a new delay cycle starts" {
                let scheduler = new ManualAsyncInvokeScheduler()
                let getScheduler onTick =
                    scheduler.Callback <- onTick
                    scheduler :> IDisposable

                let availability = System.Collections.Generic.Dictionary<string, bool>()
                availability["primary.example.com"] <- false
                availability["secondary1.example.com"] <- true

                let mutable now = t0

                let provider =
                    new AutoClusterFailover(
                        ServiceInfo(primaryUrl),
                        [| ServiceInfo(secondary1Url) |],
                        delay30s,
                        delay60s,
                        TimeSpan.FromSeconds(10.0),
                        (fun () -> now),
                        createFakeProbe availability,
                        Some getScheduler
                    ) :> IServiceInfoProvider

                let ctx, updates = createFakeContext()
                provider.Initialize(ctx)

                // Tick 1: primary down, record failure at t0
                do! scheduler.Tick()

                // Primary recovers briefly
                availability["primary.example.com"] <- true
                now <- t0 + TimeSpan.FromSeconds(10.0)
                do! scheduler.Tick() // clears failedTimestamp

                // Primary goes down again
                availability["primary.example.com"] <- false
                now <- t0 + TimeSpan.FromSeconds(15.0)
                do! scheduler.Tick() // records new failedTimestamp at t0+15

                // Original 30s from t0 would be t0+30, but because of the reset
                // the new deadline is t0+15+30 = t0+45
                now <- t0 + TimeSpan.FromSeconds(31.0)
                do! scheduler.Tick() // should NOT switch yet (only 16s since new failure)
                updates.Count |> Expect.equal "no switch yet" 0

                // Now past the new deadline
                now <- t0 + TimeSpan.FromSeconds(46.0)
                do! scheduler.Tick()
                updates.Count |> Expect.equal "now switched" 1

                provider.Dispose()
            }
        ]
