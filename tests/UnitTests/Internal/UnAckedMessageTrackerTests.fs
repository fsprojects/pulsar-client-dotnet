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



        testTask "UnAckedMessageTracker add 3 and remove until 1 works" {
            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_2", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add(msgId1)
            tracker.Add(msgId2)
            tracker.Add(msgId3)
            let! result = tracker.RemoveMessagesTill(msgId1)
            result |> Expect.equal "" 1
            tracker.Remove(msgId2)
            tracker.Remove(msgId3)
            let! result = tracker.RemoveMessagesTill(msgId3)
            result |> Expect.equal "" 0
            tracker.Close()
        }

        testTask "UnAckedMessageTracker add 3 and remove until 3 works" {
            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_3", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(10.0), emptyRedeliver, emptyScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add(msgId1)
            tracker.Add(msgId2)
            tracker.Add(msgId3)
            let! result = tracker.RemoveMessagesTill(msgId3)
            result |> Expect.equal "" 3
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

            tracker.Add(msgId1)
            tracker.Add(msgId2)
            tracker.Add(msgId3)
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

            tracker.Add(msgId1)
            tracker.Add(msgId2)
            tracker.Add(msgId3)
            tracker.Remove(msgId2)
            tracker.Remove(msgId3)
            //first two ticks just needed to get rid of 2 empty redelivery sets
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()

            let! redelivered = tsc.Task
            redelivered |> Expect.equal "" 1
            tracker.Close()
        }
    ]