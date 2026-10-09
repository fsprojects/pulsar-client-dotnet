module Pulsar.Client.UnitTests.Internal.TaskSeq

open System
open System.Collections.Generic
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.Schema
open Pulsar.Client.UnitTests


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
        
        testTask "Additional generator work well" {
            let gen1 = fun () -> task {
                do! Task.Delay(100)
                return 100
            }
            let gen2 = fun () -> task {
                do! Task.Delay(100)
                return 100
            }
            let gen3 = fun () -> task {
                do! Task.Delay(40)
                return 40
            }
            let ts = TaskSeq([gen1; gen2])
            let! result1 = ts.Next() 
            ts.AddGenerators([gen3])
            let! result2 = ts.Next() 
            let! result3 = ts.Next() 
            let! result4 = ts.Next() 
            let! result5 = ts.Next() 
            
            Expect.equal "" 100 result1
            Expect.equal "" 100 result2
            Expect.equal "" 40 result3
            Expect.equal "" 40 result4
            Expect.equal "" 100 result5
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
    ]