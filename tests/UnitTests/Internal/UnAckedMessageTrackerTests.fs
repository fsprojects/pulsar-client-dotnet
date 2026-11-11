module Pulsar.Client.UnitTests.Internal.UnAckedMessageTrackerTests

open System
open System.Threading.Tasks
open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open Pulsar.Client.UnitTests

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

        testTask "A redelivery that fails doesn't stop the tracker" {

            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable

            let tsc = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let calls = ref 0
            let redeliver msgIds =
                calls.Value <- calls.Value + 1
                if calls.Value = 1 then
                    failwith "could not redeliver"
                tsc.TrySetResult(msgIds |> Seq.length) |> ignore

            let tracker = UnAckedMessageTracker("UnAckedMessageTracker_6", TimeSpan.FromMilliseconds(50.0), TimeSpan.FromMilliseconds(25.0), redeliver, getScheduler) :> IUnAckedMessageTracker
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }

            tracker.Add(msgId1)
            //first two ticks just needed to get rid of 2 empty redelivery sets, the third one fails to redeliver
            scheduler.Tick(); scheduler.Tick(); scheduler.Tick()

            // as in the Java client, the next tick follows a failed one, and the message waits its whole ack timeout
            tracker.Add(msgId2)
            scheduler.Tick(); scheduler.Tick()
            let! _ = tracker.RemoveMessagesTill({ msgId1 with LedgerId = %0L }).WaitAsync(TimeSpan.FromSeconds(5.0))
            tsc.Task.IsCompleted |> Expect.isFalse ""
            scheduler.Tick()

            let! redelivered = tsc.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
            redelivered |> Expect.equal "" 1
            tracker.Close()
        }
    ]