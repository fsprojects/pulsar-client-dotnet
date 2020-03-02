module Pulsar.Client.UnitTests.Internal.UnAckedMessageTrackerTests

open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open System
open System.Threading.Tasks

[<Tests>]
let tests =

    let emptyRedeliver _ = ()
    let emptyScheduler _ = { new IDisposable with member __.Dispose() = () }

    testList "UnAckedMessageTracker" [

        test "UnAckedMessageTracker add and remove works" {
            let tracker = UnAckedMessageTracker("", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(10.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            tracker.Add(msgId) |> Expect.isTrue ""
            tracker.Remove(msgId) |> Expect.isTrue ""
            tracker.Close()
        }

        test "UnAckedMessageTracker add 3 and remove until 1 works" {
            let tracker = UnAckedMessageTracker("", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            tracker.RemoveMessagesTill(msgId1) |> Expect.equal "" 1
            tracker.Remove(msgId2) |> Expect.isTrue ""
            tracker.Remove(msgId3) |> Expect.isTrue ""
            tracker.Close()
        }

        test "UnAckedMessageTracker add 3 and remove until 3 works" {
            let tracker = UnAckedMessageTracker("", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(10.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            tracker.RemoveMessagesTill(msgId3) |> Expect.equal "" 3
            tracker.Remove(msgId2) |> Expect.isFalse ""
            tracker.Remove(msgId3) |> Expect.isFalse ""
            tracker.Close()
        }

        testAsync "UnAckedMessageTracker redeliver all works" {
            
            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable
            
            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)
                
            let tracker = UnAckedMessageTracker("", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), redeliver, getScheduler) :> IUnAckedMessageTracker                       
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            //first two ticks just needed to get rid of 2 empty redelivery sets
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()
            
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 3
            tracker.Close()
        }

        testAsync "UnAckedMessageTracker redeliver one works" {
            
            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable
            
            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)
                
            let tracker = UnAckedMessageTracker("", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), redeliver, getScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            tracker.Remove(msgId2) |> Expect.isTrue ""
            tracker.Remove(msgId3) |> Expect.isTrue ""
            //first two ticks just needed to get rid of 2 empty redelivery sets
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()            
            
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 1
            tracker.Close()
        }
    ]