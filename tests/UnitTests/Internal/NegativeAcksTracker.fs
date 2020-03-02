module Pulsar.Client.UnitTests.Internal.NegativeAcksTrackerTests

open FSharp.UMX
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests.Internal
open Pulsar.Client.Common
open System
open System.Collections.Concurrent
open System.Threading.Tasks

[<Tests>]
let tests =

    testList "NegativeAcksTracker" [

        //TODO this test is flaky in CI, probably because of non-deterministic await (async sleeps)
        testAsync "UnAckedMessageTracker redeliver all works" {
            
            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable
            
            let tsc = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tsc.SetResult(length)
                
            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver, getScheduler)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            
            tracker.Add msgId1 |> Expect.isTrue ""
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            
            do! Async.Sleep(150) //waiting for expiration to happen
            scheduler.Tick()     //ticking timer
            
            let! redelivered = tsc.Task |> Async.AwaitTask
            redelivered |> Expect.equal "" 3
            tracker.Close()
        }

        testAsync "UnAckedMessageTracker redeliver one and then two works" {
            
            let scheduler = new ManualInvokeScheduler()
            let getScheduler onTick =
                scheduler.Callback <- onTick
                scheduler :> IDisposable
            
            let queue = ConcurrentQueue<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                queue.Enqueue length

            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver, getScheduler)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            
            tracker.Add msgId1 |> Expect.isTrue ""
            do! Async.Sleep(150) //waiting for expiration to happen
            scheduler.Tick()     //ticking timer
            
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            do! Async.Sleep(150) //waiting for expiration
            scheduler.Tick()     //ticking timer
            
            let (_, redelivered) = queue.TryDequeue()
            redelivered |> Expect.equal "" 1
            let (_, redelivered) = queue.TryDequeue()
            redelivered |> Expect.equal "" 2
            tracker.Close()
        }
    ]