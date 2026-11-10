module Pulsar.Client.UnitTests.Common.ToolsTests

open System
open System.Collections
open System.Collections.Concurrent
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Common

type private QueueScheduler() =
    inherit TaskScheduler()
    let queue = ConcurrentQueue<Task>()
    member this.RunQueued() =
        let mutable task = null
        while queue.TryDequeue(&task) do
            this.TryExecuteTask task |> ignore
    override _.GetScheduledTasks() = queue :> seq<Task>
    override _.QueueTask task = queue.Enqueue task
    override _.TryExecuteTaskInline(_, _) = false

/// The task of a loop that the exception has escaped.
let private loopStoppedBy (ex: exn) =
    let loop: Task<unit> = backgroundTask { return raise ex }
    try
        loop.Wait()
    with _ ->
        ()
    loop :> Task

[<Tests>]
let tests =

    testList "BitSetToLongArray" [
        test "Over 64 elements" {
            let ba = BitArray(65)
            ba.Set(63, true)
            ba.Set(64, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [-9223372036854775808L; 1L] result
        }
        
        test "64 elements" {
            let ba = BitArray(64)
            ba.Set(1, true)
            ba.Set(63, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [-9223372036854775806L] result
        }
        
        test "Less than 64 elements" {
            let ba = BitArray(3)
            ba.Set(1, true)
            ba.Set(2, true)
            let result = ba |> toLongArray |> Array.toList
            Expect.equal "" [6L] result
        }
        
        test "Reverse Over 64 elements" {
            let ba = BitArray(65)
            ba.Set(63, true)
            ba.Set(64, true)
            let result = fromLongArray [| -9223372036854775808L; 1L|] 65
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        test "Reverse 64 elements" {
            let ba = BitArray(64)
            ba.Set(1, true)
            ba.Set(63, true)
            let result = fromLongArray [|-9223372036854775806L|] 64
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        test "Reverse Less than 64 elements" {
            let ba = BitArray(3)
            ba.Set(1, true)
            ba.Set(2, true)
            let result = fromLongArray [|6L|] 3
            let equalityResult = ba.Xor(result)
            for i in 0..equalityResult.Count-1 do
                Expect.isFalse "" equalityResult.[i]
        }
        
        testTask "Async delay shouldn't crash process" {
            let x = true
            let f = fun () -> failwith "failure"
            asyncDelayMs 20 f
            do! Task.Delay 100
            do! Task.Delay 100
            Console.WriteLine(x)
            Expect.isTrue "" x
        }

        test "postAndAsyncReply runs continuations asynchronously" {
            let channel = Channel.CreateUnbounded<TaskCompletionSource<int>>()
            let task = postAndAsyncReply channel id
            let tcs = channel.Reader.ReadAsync().AsTask().Result
            let replyThread = Thread.CurrentThread.ManagedThreadId
            let continuationThread = ref replyThread
            use continued = new ManualResetEventSlim()
            task.ContinueWith((fun (_: Task<int>) ->
                continuationThread.Value <- Thread.CurrentThread.ManagedThreadId
                continued.Set()), TaskContinuationOptions.ExecuteSynchronously) |> ignore
            tcs.SetResult 1
            continued.Wait()
            Expect.notEqual "" replyThread continuationThread.Value
            Expect.equal "" TaskCreationOptions.RunContinuationsAsynchronously task.CreationOptions
        }

        test "runContinuationsAsynchronously does not depend on the caller's scheduler" {
            let scheduler = QueueScheduler()
            let tcs = TaskCompletionSource<int>()
            let mirrored = Task.Factory.StartNew((fun () -> runContinuationsAsynchronously tcs.Task), CancellationToken.None, TaskCreationOptions.None, scheduler)
            scheduler.RunQueued()
            tcs.SetResult 1
            Expect.isTrue "" (mirrored.Result.Wait(TimeSpan.FromSeconds 5.0))
        }

        test "loopFailure is null for a loop that ended by itself" {
            Expect.isNull "" (loopFailure Task.CompletedTask)
        }

        test "loopFailure is the exception that stopped a loop" {
            let failure = InvalidOperationException "the loop failed"
            let loop = loopStoppedBy failure
            Expect.isTrue "" loop.IsFaulted
            Expect.isTrue "" (obj.ReferenceEquals(loopFailure loop, failure))
        }

        test "loopFailure holds the cancellation that stopped a loop" {
            let cancellation = OperationCanceledException "a call timed out"
            let loop = loopStoppedBy cancellation
            Expect.isTrue "The loop ends cancelled, not faulted" loop.IsCanceled
            let failure = loopFailure loop
            // a caller that failed with the cancellation itself would end cancelled in turn
            Expect.isFalse "" (failure :? OperationCanceledException)
            Expect.isTrue "" (obj.ReferenceEquals(failure.InnerException, cancellation))
        }
    ]
    
 