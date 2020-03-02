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
            
            do! Async.Sleep(105) //waiting for expiration to happen
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
            
            let mutable tcs = TaskCompletionSource<int>()
            let redeliver msgIds =
                let length = msgIds |> Seq.length
                tcs.TrySetResult length |> ignore

            let tracker = NegativeAcksTracker("", TimeSpan.FromMilliseconds(100.0), redeliver, getScheduler)
            let msgId1 = { LedgerId = %1L; EntryId = %1L;  Partition = 1; Type = Individual; TopicName = %"" }
            let msgId2 = { msgId1 with EntryId = %2L }
            let msgId3 = { msgId1 with EntryId = %3L }
            
            tracker.Add msgId1 |> Expect.isTrue ""
            do! Async.Sleep(35)  //waiting for step expiration to happen
            scheduler.Tick()     //ticking timer
            
            tracker.Add msgId2 |> Expect.isTrue ""
            tracker.Add msgId3 |> Expect.isTrue ""
            do! Async.Sleep(70)  //waiting for double step expiration
            scheduler.Tick()     //ticking timer
            
            let! redelivered1 = tcs.Task |> Async.AwaitTask
            redelivered1 |> Expect.equal "" 1
            
            tcs <- TaskCompletionSource<int>()
            do! Async.Sleep(35)  //waiting for one more stop expiration to happen
            scheduler.Tick()     //ticking timer
            let! redelivered2 = tcs.Task |> Async.AwaitTask
            redelivered2 |> Expect.equal "" 2
            tracker.Close()
        }
    ]