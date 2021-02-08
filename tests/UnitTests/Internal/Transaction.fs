module Pulsar.Client.UnitTests.Internal.Transaction

open System
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open Pulsar.Client.Transaction
open FSharp.UMX


[<Tests>]
let tests =
    
    ftestList "Transaction tests" [
        
        testAsync "Registering topic multiple times with same subscription returns same task" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun (_, _) -> Task.FromResult()
                    Abort = fun (_, _) -> Task.FromResult()
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
        
        testAsync "Registering topic multiple times with different subscriptions returns different tasks" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun (_, _) -> Task.FromResult()
                    Abort = fun (_, _) -> Task.FromResult()
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
        
        testAsync "Registering producer to the same topic returns same task" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun (_, _) -> Task.FromResult()
                    Abort = fun (_, _) -> Task.FromResult()
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
        
        testAsync "Registering producers to multiple topics returns different tasks" {
            
            let timeout = TimeSpan.FromMinutes(1.0)
            let operations =
                {
                    AddPublishPartitionToTxn = fun (_, _) -> Task.FromResult()
                    AddSubscriptionToTxn = fun (_, _, _) -> Task.FromResult()
                    Commit = fun (_, _) -> Task.FromResult()
                    Abort = fun (_, _) -> Task.FromResult()
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
    ]