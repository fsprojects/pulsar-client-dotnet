module Pulsar.Client.UnitTests.Internal.TaskSeq

open System
open System.Collections.Generic
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.Schema
open Pulsar.Client.UnitTests


/// A generator whose tasks the test completes itself, so the order they complete in doesn't depend on timers.
type private ControlledGenerator<'T>() =
    let handedOut = Channel.CreateUnbounded<TaskCompletionSource<'T>>()
    let generator: TaskGenerator<'T> =
        fun () ->
            let source = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
            handedOut.Writer.TryWrite source |> ignore
            source.Task
    member _.Generator = generator
    /// Completes the generator's oldest task that is still pending, once the loop has taken it.
    member _.Complete(result: 'T) =
        task {
            let! source = handedOut.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5.0))
            source.SetResult result
        }

let private failureOf (request: Task) =
    task {
        let! finished = Task.WhenAny(request, Task.Delay(TimeSpan.FromSeconds(5.0)))
        Expect.isTrue "The request must not wait for a loop that has stopped" (obj.ReferenceEquals(finished, request))
        Expect.isTrue "The request must fail" request.IsFaulted
        return request.Exception.GetBaseException()
    }

let private failsWith (cause: exn) (request: Task) =
    task {
        let! (failure: exn) = failureOf request
        Expect.isTrue "The request must fail with what stopped the loop" (obj.ReferenceEquals(failure, cause))
    }

[<Tests>]
let tests =
    
    testList "TaskSeq tests" [

        testTask "Empty input doesn't throw" {
            let ts = TaskSeq(Seq.empty)
            let task1 = task {
                let! _ = ts.Next()
                return 1
            }
            let task2 = task {
                let! _ = Task.Delay(100)
                return 2
            }
                        
            let! (resultTask : Task<int>) = Task.WhenAny(task1, task2)
            
            let! result = resultTask
            
            Expect.equal "" 2 result
        }
        
        testTask "Initial generators work well" {
            let gen1 = fun () -> task {
                do! Task.Delay(100)
                return 100
            }
            let gen2 = fun () -> task {
                do! Task.Delay(40)
                return 40
            }            
            let ts = TaskSeq([gen1; gen2])
            let! result1 = ts.Next() 
            let! result2 = ts.Next() 
            let! result3 = ts.Next() 
            let! result4 = ts.Next() 
            
            Expect.equal "" 40 result1
            Expect.equal "" 40 result2
            Expect.equal "" 100 result3
            Expect.equal "" 40 result4
        }
        
        // The test completes the tasks itself. With delays, the added generator's second task was due 20 ms before
        // the next task of an initial one, which is about one tick of the timer on Windows, so they could swap.
        testTask "Additional generator work well" {
            let gen1 = ControlledGenerator<int>()
            let gen2 = ControlledGenerator<int>()
            let gen3 = ControlledGenerator<int>()
            let ts = TaskSeq([gen1.Generator; gen2.Generator])
            let next () = ts.Next().WaitAsync(TimeSpan.FromSeconds(5.0))

            let next1 = next ()
            do! gen1.Complete 1
            let! result1 = next1
            // completes while nothing waits for it, and before the generator is added
            do! gen2.Complete 2
            ts.AddGenerators([gen3.Generator])
            let! result2 = next ()
            let next3 = next ()
            do! gen3.Complete 3
            let! result3 = next3
            // the added generator is asked for another task, as the initial ones are
            let next4 = next ()
            do! gen3.Complete 3
            let! result4 = next4
            let next5 = next ()
            do! gen1.Complete 1
            let! result5 = next5

            Expect.equal "" 1 result1
            Expect.equal "" 2 result2
            Expect.equal "" 3 result3
            Expect.equal "" 3 result4
            Expect.equal "" 1 result5
        }

        testTask "Add/Remove generator work well" {
            let gen1 = fun () -> task {
                do! Task.Delay(100)
                return 100
            }
            let gen2 = fun () -> task {
                do! Task.Delay(40)
                return 40
            }
            let ts = TaskSeq<int>(Seq.empty)
            let result1Task = ts.Next()
            ts.AddGenerators([gen1; gen2])
            let! result2 = ts.Next() 
            let! result3 = ts.Next() 
            do! ts.RemoveGenerator(gen2)
            let! result4 = ts.Next() 
            
            Expect.equal "" 40 result1Task.Result
            Expect.equal "" 40 result2
            Expect.equal "" 100 result3
            Expect.equal "" 100 result4
        }

        testTask "Removing a pending generator excludes its later failure" {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let removed = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let remaining = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let removedGenerator () =
                started.TrySetResult() |> ignore
                removed.Task
            let ts = TaskSeq([removedGenerator; fun () -> remaining.Task])
            let pending = ts.Next()
            do! started.Task.WaitAsync(TimeSpan.FromSeconds(5.0))

            do! ts.RemoveGenerator removedGenerator
            removed.SetException(AlreadyClosedException "Removed child was disposed")
            remaining.SetResult 42

            let! result = pending.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "A removed child's failure must not escape the pending Next" 42 result
        }

        testTask "Removing the last generator preserves waiters for later additions" {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let removed = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let removedGenerator () =
                started.TrySetResult() |> ignore
                removed.Task
            let ts = TaskSeq([removedGenerator])
            let first = ts.Next()
            let second = ts.Next()
            do! started.Task.WaitAsync(TimeSpan.FromSeconds(5.0))

            do! ts.RemoveGenerator removedGenerator
            removed.SetException(AlreadyClosedException "Last child was disposed")
            ts.AddGenerators([fun () -> Task.FromResult 42])

            let! results = Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "Queued Next calls must resume from the new generator" [|42; 42|] results
        }
        
        testTask "Adding generator resets waitAny" {
            
            let gen1 = fun () -> task {
                do! Task.Delay(1000)
                return 1000
            }
            let gen2 = fun () -> task {
                do! Task.Delay(400)
                return 400
            }
            let ts = TaskSeq<int>([gen1])
            let result1Task = ts.Next()
            ts.AddGenerators([gen2])
            let! result2 = ts.Next() 
            let! result3 = ts.Next() 
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 400 result2
            Expect.equal "" 1000 result3
        }
        
        testTask "Parallel waiters work fine" {
            let gen1 = fun () -> task {
                do! Task.Delay(550)
                return 550
            }
            let gen2 = fun () -> task {
                do! Task.Delay(500)
                return 500
            }
            let gen3 = fun () -> task {
                do! Task.Delay(450)
                return 450
            }
            let gen4 = fun () -> task {
                do! Task.Delay(400)
                return 400
            }
            let ts = TaskSeq<int>([gen1; gen2])
            let result1Task = ts.Next()
            let result2Task = ts.Next()
            let result3Task = ts.Next()
            ts.AddGenerators([gen3; gen4])
            let! result4 = ts.Next() 
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 450 result2Task.Result
            Expect.equal "" 500 result3Task.Result
            Expect.equal "" 550 result4
        }
        
        testTask "Multiple add generators works fine" {
            let gen1 = fun () -> task {
                do! Task.Delay(550)
                return 550
            }
            let gen2 = fun () -> task {
                do! Task.Delay(500)
                return 500
            }
            let gen3 = fun () -> task {
                do! Task.Delay(450)
                return 450
            }
            let gen4 = fun () -> task {
                do! Task.Delay(400)
                return 400
            }
            let ts = TaskSeq<int>([gen1; gen2])
            let result1Task = ts.Next()
            let result2Task = ts.Next()
            let result3Task = ts.Next()
            ts.AddGenerators([gen3])
            ts.AddGenerators([gen4])
            let! result4 = ts.Next() 
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 450 result2Task.Result
            Expect.equal "" 500 result3Task.Result
            Expect.equal "" 550 result4
        }
        
        testTask "Completed tasks come in unpredicted order" {
            
            let gen1 = fun () -> Task.FromResult(1)
            let gen2 = fun () -> Task.FromResult(2)
            let gen3 = fun () -> Task.FromResult(3)
            let gen4 = fun () -> Task.FromResult(4)
            
            
            let ts = TaskSeq<int>([gen1; gen2; gen3; gen4])
            let results1 = [|
                for _ in 1..10 do
                    ts.Next().Result
            |]
            let results2 = [|
                for _ in 1..10 do
                    ts.Next().Result
            |]
            
            Expect.notEqual "" results1 results2 
        }

        // The loop calls the generators itself, so one that throws instead of returning a task stops the loop,
        // as an OutOfMemoryException does when a task can't be allocated.
        testTask "Requests fail with what stopped the loop instead of waiting for it" {
            let failure = InvalidOperationException "generator could not start"
            let pending: TaskGenerator<int> = fun () -> TaskCompletionSource<int>().Task
            let failing: TaskGenerator<int> = fun () -> raise failure
            let ts = TaskSeq([pending; failing])

            // the request that made the loop start the generators
            do! failsWith failure (ts.Next())
            // requests made after the loop has stopped: a multi-topic consumer's poller, and its mailbox
            do! failsWith failure (ts.Next())
            do! failsWith failure (ts.RemoveGenerator pending)
        }

        testTask "A request waiting for its turn fails when the loop stops" {
            let failure = InvalidOperationException "generator could not be re-armed"
            let first = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let calls = ref 0
            let generator: TaskGenerator<int> =
                fun () ->
                    calls.Value <- calls.Value + 1
                    if calls.Value = 1 then first.Task else raise failure
            let ts = TaskSeq([generator])

            let delivered = ts.Next()
            // answered only after the first request has taken its task
            let waiting = ts.Next()
            first.SetResult 1

            let! result = delivered.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "The task that completed is delivered" 1 result
            // the loop stops when it calls the generator again
            do! failsWith failure waiting
        }

        testTask "A request waiting for the generators' tasks fails when the loop stops" {
            let failure = InvalidOperationException "generator could not start"
            let pending: TaskGenerator<int> = fun () -> TaskCompletionSource<int>().Task
            let failing: TaskGenerator<int> = fun () -> raise failure
            let ts = TaskSeq([pending])

            // it waits for the task of the only generator, which never completes
            let waiting = ts.Next()
            // as when a multi-topic consumer starts reading the consumer of a new topic
            ts.AddGenerators([failing])

            do! failsWith failure waiting
        }

        testTask "Requests fail when what stopped the loop is a cancellation" {
            let cancellation = OperationCanceledException "generator was cancelled"
            let pending: TaskGenerator<int> = fun () -> TaskCompletionSource<int>().Task
            let cancelled: TaskGenerator<int> = fun () -> raise cancellation
            let ts = TaskSeq([pending; cancelled])
            // A request that failed with the cancellation itself would end cancelled, which its caller could take
            // for a normal stop, as the loop's own task ends cancelled instead of faulted.
            let failsHoldingTheCancellation (request: Task) =
                task {
                    let! (failure: exn) = failureOf request
                    Expect.isTrue "The failure must hold what stopped the loop"
                        (obj.ReferenceEquals(failure.InnerException, cancellation))
                }

            do! failsHoldingTheCancellation (ts.Next())
            do! failsHoldingTheCancellation (ts.Next())
            do! failsHoldingTheCancellation (ts.RemoveGenerator pending)
        }

        // The sequence cancels a task of its own to make a waiting request look at the tasks again. A generator's
        // task that ends cancelled is not that signal. Taken for it, the task would be found again and again.
        testTask "A generator's cancelled task is delivered like any other" {
            let first = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
            let calls = ref 0
            let generator: TaskGenerator<int> =
                fun () ->
                    calls.Value <- calls.Value + 1
                    if calls.Value = 1 then first.Task else Task.FromResult 2
            let ts = TaskSeq([generator])

            let cancelled = ts.Next()
            first.SetCanceled()
            let! finished = Task.WhenAny(cancelled :> Task, Task.Delay(TimeSpan.FromSeconds(5.0)))
            Expect.isTrue "The request must not go on looking at a task that has ended"
                (obj.ReferenceEquals(finished, cancelled))
            Expect.isTrue "The request ends as the task did" cancelled.IsCanceled

            // the generator is asked for its next task, as after any other
            let! next = ts.Next().WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.equal "" 2 next
        }
    ]