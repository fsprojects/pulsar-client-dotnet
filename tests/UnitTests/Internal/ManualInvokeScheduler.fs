namespace Pulsar.Client.UnitTests.Internal

open System

/// This is ad-hoc scheduler for unit testing purposes for manually controlling when timer ticks happen
type internal ManualInvokeScheduler() =
    member val Callback = fun () -> () with get, set
    member this.Tick() = this.Callback()
    interface IDisposable with
        member __.Dispose() = ()
