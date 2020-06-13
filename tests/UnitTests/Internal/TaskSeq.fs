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
open FSharp.Control.Tasks.V2.ContextInsensitive

[<Tests>]
let tests =
    
    testList "TaskSeq tests" [

        testAsync "Empty input doesn't throw" {
            let ts = TaskSeq(Seq.empty)
            let task1 = task {
                let! _ = ts.Next()
                return 1
            }
            let task2 = task {
                let! _ = Task.Delay(100)
                return 2
            }
            let! result = Task.WhenAny(task1, task2) |> Async.AwaitTask
            Expect.equal "" 2 result.Result
        }
        
        testAsync "Initial generators work well" {
            let gen1 = fun () -> task {
                do! Task.Delay(100)
                return 100
            }
            let gen2 = fun () -> task {
                do! Task.Delay(40)
                return 40
            }            
            let ts = TaskSeq([gen1; gen2])
            let! result1 = ts.Next() |> Async.AwaitTask
            let! result2 = ts.Next() |> Async.AwaitTask
            let! result3 = ts.Next() |> Async.AwaitTask
            let! result4 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 40 result1
            Expect.equal "" 40 result2
            Expect.equal "" 100 result3
            Expect.equal "" 40 result4
        }
        
        testAsync "Additional generator work well" {
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
            let! result1 = ts.Next() |> Async.AwaitTask
            ts.AddGenerators([gen3])
            let! result2 = ts.Next() |> Async.AwaitTask
            let! result3 = ts.Next() |> Async.AwaitTask
            let! result4 = ts.Next() |> Async.AwaitTask
            let! result5 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 100 result1
            Expect.equal "" 100 result2
            Expect.equal "" 40 result3
            Expect.equal "" 40 result4
            Expect.equal "" 100 result5
        }
        
        testAsync "Add/Remove generator work well" {
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
            let! result2 = ts.Next() |> Async.AwaitTask
            let! result3 = ts.Next() |> Async.AwaitTask
            ts.RemoveGenerator(gen2)
            let! result4 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 40 result1Task.Result
            Expect.equal "" 40 result2
            Expect.equal "" 100 result3
            Expect.equal "" 100 result4
        }
        
        testAsync "Adding generator resets waitAny" {
            
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
            let! result2 = ts.Next() |> Async.AwaitTask
            let! result3 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 400 result2
            Expect.equal "" 1000 result3
        }
        
        testAsync "Parallel waiters work fine" {
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
            let! result4 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 450 result2Task.Result
            Expect.equal "" 500 result3Task.Result
            Expect.equal "" 550 result4
        }
        
        testAsync "Multiple add generators works fine" {
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
            let! result4 = ts.Next() |> Async.AwaitTask
            
            Expect.equal "" 400 result1Task.Result
            Expect.equal "" 450 result2Task.Result
            Expect.equal "" 500 result3Task.Result
            Expect.equal "" 550 result4
        }
    ]