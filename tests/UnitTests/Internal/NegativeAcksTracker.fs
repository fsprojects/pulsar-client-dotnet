module Pulsar.Client.UnitTests.Internal.NegativeAcksTrackerTests

open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open System
open System.Threading.Tasks

[<Tests>]
let tests =

    testList "NegativeAcksTracker" [

        testAsync "UnAckedMessageTracker redeliver all works" {
            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)
            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 3
            tracker.Close()
        }

        testAsync "UnAckedMessageTracker redeliver one and then two works" {
            let mutable tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)
            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            tracker.Add msgId1 |> Expect.isTrue ""
            do! Async.Sleep(50)
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 1
            tsc <- TaskCompletionSource<int>()
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 2
            tracker.Close()
        }
    ]