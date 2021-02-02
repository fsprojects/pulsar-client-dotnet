module Pulsar.Client.IntegrationTests.Transaction

open System
open System.Threading
open System.Diagnostics

open Expecto
open Expecto.Flip
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =

    testList "Transaction" [
        
        ptestAsync "Send and receive 10 messages sequentially works fine with txn" {

            Log.Debug("Started Send and receive 10 messages concurrently works fine with txn")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            
            let! txn =
                client.NewTransaction().BuildAsync()
                |> Async.AwaitTask

            let! producer =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName("txnProduce")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    

            let! consumer =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName("txnProduce")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            Log.Debug("Before produce")
            do! produceMessagesWithTxn producer txn numberOfMessages "txnProduce" |> Async.AwaitTask
                
            Log.Debug("Before commit")
            do! txn.Commit() |> Async.AwaitTask
            Log.Debug("After commit")
            
            do! consumeMessages consumer numberOfMessages "txnProduce" |> Async.AwaitTask

            Log.Debug("Finished Send and receive 10 messages sequentially works fine with txn")
        }
    ]