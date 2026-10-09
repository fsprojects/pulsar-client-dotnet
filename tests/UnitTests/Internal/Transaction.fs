module Pulsar.Client.UnitTests.Internal.Transaction

open System
open System.Threading
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open Pulsar.Client.Transaction
open FSharp.UMX



[<Tests>]
let tests =
    
    testList "Transaction tests" [
        
        testTask "Registering topic multiple times with same subscription returns same task" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            
            let ts = Transaction(timeout, operations, txnId)
            let topicName = %"123"
            let subscription = %"abcd"
            let results =
                [|
                  ts.RegisterAckedTopic(topicName, subscription)
                  ts.RegisterAckedTopic(topicName, subscription)
                  ts.RegisterAckedTopic(topicName, subscription)
                |]
            Expect.allEqual "" results.[0] results
        }
        
        testTask "Registering topic multiple times with different subscriptions returns different tasks" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> task {
                        do! Task.Yield()
                        return ()
                    }
                    AddSubscriptionToTxn = fun (_, _, _) -> task {
                        do! Task.Yield()
                        return ()
                    }
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            
            let ts = Transaction(timeout, operations, txnId)
            let topicName1 = %"123"
            let topicName2 = %"321"
            let subscription1 = %"abcd"
            let subscription2 = %"dbca"
            let results =
                [|
                  ts.RegisterAckedTopic(topicName1, subscription1)
                  ts.RegisterAckedTopic(topicName1, subscription2)
                  ts.RegisterAckedTopic(topicName2, subscription1)
                |]
            Expect.notEqual "" results.[0] results.[1]
            Expect.notEqual "" results.[1] results.[2]
            Expect.notEqual "" results.[0] results.[2]
        }
        
        testTask "Registering producer to the same topic returns same task" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            
            let ts = Transaction(timeout, operations, txnId)
            let topicName = %"123"
            let results =
                [|
                  ts.RegisterProducedTopic(topicName)
                  ts.RegisterProducedTopic(topicName)
                  ts.RegisterProducedTopic(topicName)
                |]
            Expect.allEqual "" results.[0] results
        }
        
        testTask "Registering producers to multiple topics returns different tasks" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> task {
                        do! Task.Yield()
                        return ()
                    }
                    AddSubscriptionToTxn = fun (_, _, _) -> task {
                        do! Task.Yield()
                        return ()
                    }
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            
            let ts = Transaction(timeout, operations, txnId)
            let topicName1 = %"123"
            let topicName2 = %"321"
            
            let results =
                [|
                  ts.RegisterProducedTopic(topicName1)
                  ts.RegisterProducedTopic(topicName2)
                |]
            Expect.notEqual "" results.[0] results.[1]
        }
        
        testTask "Ack and commit works as expected" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let mutable commited = false
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> commited <- true; Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let tcs = TaskCompletionSource<Unit>()
            let ts = Transaction(timeout, operations, txnId)
            ts.RegisterAckOp(tcs.Task)
            let commitTask = ts.Commit()
            do! Task.Delay 40
            Expect.isFalse "" commitTask.IsCompleted
            tcs.SetResult()
            do! Task.Delay 40
            Expect.isTrue "" commitTask.IsCompletedSuccessfully
            Expect.isTrue "" commited
        }
        
        testTask "Send and commit works as expected" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let mutable commited = false
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> commited <- true; Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let tcs = TaskCompletionSource<MessageId>()
            let ts = Transaction(timeout, operations, txnId)
            ts.RegisterSendOp(tcs.Task)
            let commitTask = ts.Commit()
            do! Task.Delay 40
            Expect.isFalse "" commitTask.IsCompleted
            tcs.SetResult(MessageId.Latest)
            do! Task.Delay 40
            Expect.isTrue "" commitTask.IsCompletedSuccessfully
            Expect.isTrue "" commited
        }
        
        testTask "Ack and abort works as expected" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let mutable aborted = false
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> aborted <- true;  Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let tcs = TaskCompletionSource<Unit>()
            let ts = Transaction(timeout, operations, txnId)
            ts.RegisterAckOp(tcs.Task)
            let commitTask = ts.Abort()
            do! Task.Delay 40
            Expect.isFalse "" commitTask.IsCompleted
            tcs.SetResult()
            do! Task.Delay 40
            Expect.isTrue "" commitTask.IsCompletedSuccessfully
            Expect.isTrue "" aborted
        }
        
        testTask "Send and abort works as expected" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let mutable aborted = false
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> aborted <- true;  Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let tcs = TaskCompletionSource<MessageId>()
            let ts = Transaction(timeout, operations, txnId)
            ts.RegisterSendOp(tcs.Task)
            let commitTask = ts.Abort()
            do! Task.Delay 40
            Expect.isFalse "" commitTask.IsCompleted
            tcs.SetResult(MessageId.Latest)
            do! Task.Delay 40
            Expect.isTrue "" commitTask.IsCompletedSuccessfully
            Expect.isTrue "" aborted
        }
        
        testTask "Can't use transaction after commit" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let ts = Transaction(timeout, operations, txnId)
            ts.Commit() |> ignore
            Expect.throws "" (fun () -> ts.Commit().Result)
            Expect.throws "" (fun () -> ts.Abort().Result)
            Expect.throws "" (fun () -> ts.RegisterAckedTopic(%"topic", %"subscription").Result)
            Expect.throws "" (fun () -> ts.RegisterProducedTopic(%"topic").Result)
            
        }
        
        testTask "Can't use transaction after abort" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let ts = Transaction(timeout, operations, txnId)
            ts.Abort() |> ignore
            Expect.throws "" (fun () -> ts.Commit().Result)
            Expect.throws "" (fun () -> ts.Abort().Result)
            Expect.throws "" (fun () -> ts.RegisterAckedTopic(%"topic", %"subscription").Result)
            Expect.throws "" (fun () -> ts.RegisterProducedTopic(%"topic").Result)
            
        }
        
        testTask "CumulativeAckConsumer is cleared after abort" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun _ -> Task.FromResult()
                    Abort = fun _ -> Task.FromResult()
                }
            let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }
            let ts = Transaction(timeout, operations, txnId)
            
            let msgToClearNum = 3
            let mutable increased = 0
            let consumerOperations =
                {
                    ClearIncomingMessagesAndGetMessageNumber = fun () -> async { return msgToClearNum }
                    IncreaseAvailablePermits = fun num -> increased <- num
                }
            ts.RegisterCumulativeAckConsumer(%1UL, consumerOperations)
            do! ts.Abort() 
            
            Expect.equal "" msgToClearNum increased
        }

        let operations =
            {
                AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                Commit = fun _ -> Task.FromResult()
                Abort = fun _ -> Task.FromResult()
            }
        let txnId = { LeastSigBits = 1UL; MostSigBits = 2UL }

        let continuationThread (finish: Transaction -> Task<unit>) =
            let tcs = TaskCompletionSource<Unit>()
            let ts = Transaction(TimeSpan.FromMinutes(1.0), operations, txnId)
            ts.RegisterAckOp(tcs.Task)
            let continuationThread = ref 0
            use continued = new ManualResetEventSlim()
            (finish ts).ContinueWith((fun (_: Task<unit>) ->
                continuationThread.Value <- Thread.CurrentThread.ManagedThreadId
                continued.Set()), TaskContinuationOptions.ExecuteSynchronously) |> ignore
            tcs.SetResult()
            continued.Wait()
            continuationThread.Value

        test "Commit continuations run asynchronously" {
            let thread = continuationThread (fun ts -> ts.Commit())
            Expect.notEqual "" Thread.CurrentThread.ManagedThreadId thread
        }

        test "Abort continuations run asynchronously" {
            let thread = continuationThread (fun ts -> ts.Abort())
            Expect.notEqual "" Thread.CurrentThread.ManagedThreadId thread
        }

        test "Commit fails with the operation exception and aborts" {
            let committed = ref false
            let operations = { operations with Commit = fun _ -> committed.Value <- true; Task.FromResult() }
            let tcs = TaskCompletionSource<Unit>()
            let ts = Transaction(TimeSpan.FromMinutes(1.0), operations, txnId)
            ts.RegisterAckOp(tcs.Task)
            let commitTask = ts.Commit()
            tcs.SetException(TimeoutException())
            Expect.throwsT<TimeoutException> "" (fun () -> commitTask.GetAwaiter().GetResult())
            Expect.isFalse "" committed.Value
            Expect.equal "" ABORTED ts.State
        }

        test "Abort fails with the abort exception" {
            let operations = { operations with Abort = fun _ -> Task.FromException<unit>(TimeoutException()) }
            let ts = Transaction(TimeSpan.FromMinutes(1.0), operations, txnId)
            Expect.throwsT<TimeoutException> "" (fun () -> ts.Abort().GetAwaiter().GetResult())
        }

        test "Commit fails with the operation exception when abort fails" {
            let operations = { operations with Abort = fun _ -> Task.FromException<unit>(InvalidOperationException()) }
            let tcs = TaskCompletionSource<Unit>()
            let ts = Transaction(TimeSpan.FromMinutes(1.0), operations, txnId)
            ts.RegisterAckOp(tcs.Task)
            let commitTask = ts.Commit()
            tcs.SetException(TimeoutException())
            Expect.throwsT<TimeoutException> "" (fun () -> commitTask.GetAwaiter().GetResult())
        }
    ]