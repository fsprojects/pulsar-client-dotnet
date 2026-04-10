namespace Pulsar.Client.UnitTests

open System
open System.Threading.Tasks

/// This is ad-hoc scheduler for unit testing purposes for manually controlling when timer ticks happen
type internal ManualInvokeScheduler() =
    member val Callback = fun () -> () with get, set
    member this.Tick() = this.Callback()
    interface IDisposable with
        member _.Dispose() = ()

/// This is ad-hoc scheduler for unit testing purposes for manually controlling when timer ticks happen
type internal ManualAsyncInvokeScheduler() =
    member val Callback = fun () -> Task.FromResult() with get, set
    member this.Tick() = this.Callback()
    interface IDisposable with
        member _.Dispose() = ()
