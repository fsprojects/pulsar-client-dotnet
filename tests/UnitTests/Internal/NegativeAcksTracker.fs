module Pulsar.Client.UnitTests.Internal.NegativeAcksTrackerTests

open System
open System.Threading.Tasks
open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal
open Pulsar.Client.Common

[<Tests>]
let tests =

    testList "NegativeAcksTracker" [

        testTask "UnAckedMessageTracker redeliver all works" {

            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable

            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)

            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver, getScheduler)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }

            let! result = tracker.Add msgId1
            result |> Expect.isTrue ""
            let! result = tracker.Add msgId2
            result |> Expect.isTrue ""
            let! result = tracker.Add msgId3
            result |> Expect.isTrue ""

            do! Task.Delay(120) //waiting for expiration to happen
            scheduler.Tick()     //ticking timer

            let! redelivered = tsc.Task
            redelivered |> Expect.equal "" 3
            tracker.Close()
        }

        testTask "UnAckedMessageTracker redeliver one and then two works" {

            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable

            let mutable tcs = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tcs.TrySetResult length |> ignore

            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver, getScheduler)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }

            let! result = tracker.Add msgId1
            result |> Expect.isTrue ""
            do! Task.Delay(35)  //waiting for step expiration to happen
            scheduler.Tick()     //ticking timer

            let! result = tracker.Add msgId2
            result |> Expect.isTrue ""
            let! result = tracker.Add msgId3
            result |> Expect.isTrue ""
            do! Task.Delay(70)  //waiting for double step expiration
            scheduler.Tick()     //ticking timer

            let! redelivered1 = tcs.Task
            redelivered1 |> Expect.equal "" 1

            tcs <- TaskCompletionSource<int>()
            do! Task.Delay(35)  //waiting for one more stop expiration to happen
            scheduler.Tick()     //ticking timer
            let! redelivered2 = tcs.Task
            redelivered2 |> Expect.equal "" 2
            tracker.Close()
        }
    ]