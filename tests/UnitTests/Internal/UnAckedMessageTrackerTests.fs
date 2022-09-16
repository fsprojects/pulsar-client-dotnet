module Pulsar.Client.UnitTests.Internal.UnAckedMessageTrackerTests

open System
open System.Threading.Tasks
open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common

[<Tests>]
let tests =

    let emptyRedeliver _ = ()
    let emptyScheduler _ = { new IDisposable with member __.Dispose() = () }

    testList "UnAckedMessageTracker" [

        testTask "UnAckedMessageTracker add and remove works" {
            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_1", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(10.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let! result = tracker.Add(msgId)
            result |> Expect.isTrue ""
            let! result = tracker.Remove(msgId)
            result |> Expect.isTrue ""
            tracker.Close()
        }

        testTask "UnAckedMessageTracker add 3 and remove until 1 works" {
            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_2", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            let! result = tracker.Add(msgId1)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId3)
            result |> Expect.isTrue ""
            let! result = tracker.RemoveMessagesTill(msgId1)
            result |> Expect.equal "" 1
            let! result = tracker.Remove(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Remove(msgId3)
            result |> Expect.isTrue ""
            tracker.Close()
        }

        testTask "UnAckedMessageTracker add 3 and remove until 3 works" {
            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_3", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(10.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            let! result = tracker.Add(msgId1)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId3)
            result |> Expect.isTrue ""
            let! result = tracker.RemoveMessagesTill(msgId3)
            result |> Expect.equal "" 3
            let! result = tracker.Remove(msgId2)
            result |> Expect.isFalse ""
            let! result = tracker.Remove(msgId3)
            result |> Expect.isFalse ""
            tracker.Close()
        }

        testTask "UnAckedMessageTracker redeliver all works" {

            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable

            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)

            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_4", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), redeliver, getScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }

            let! result = tracker.Add(msgId1)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId3)
            result |> Expect.isTrue ""
            //first two ticks just needed to get rid of 2 empty redelivery sets
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()

            let! redelivered = tsc.Task
            redelivered |> Expect.equal "" 3
            tracker.Close()
        }

        testTask "UnAckedMessageTracker redeliver one works" {

            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable

            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)

            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_5", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), redeliver, getScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }

            let! result = tracker.Add(msgId1)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Add(msgId3)
            result |> Expect.isTrue ""
            let! result = tracker.Remove(msgId2)
            result |> Expect.isTrue ""
            let! result = tracker.Remove(msgId3)
            result |> Expect.isTrue ""
            //first two ticks just needed to get rid of 2 empty redelivery sets
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()

            let! redelivered = tsc.Task
            redelivered |> Expect.equal "" 1
            tracker.Close()
        }
    ]