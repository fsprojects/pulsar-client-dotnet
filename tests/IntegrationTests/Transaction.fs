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
        
        testAsync "Send and receive 10 messages sequentially works fine with txn" {

            Log.Debug("Started Send and receive 10 messages concurrently works fine with txn")
            let client = getTxnClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let name = "txnProduce"

            let! consumer =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask
            
            
            let! producer =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .SendTimeout(TimeSpan.Zero)
                    .CreateAsync() |> Async.AwaitTask
           
            
            let! txn =
                client.NewTransaction().BuildAsync()
                |> Async.AwaitTask
          
            do! produceMessagesWithTxn producer txn numberOfMessages name |> Async.AwaitTask
            
            do! txn.Commit() |> Async.AwaitTask
            
            do! consumeMessages consumer numberOfMessages name |> Async.AwaitTask

            Log.Debug("Finished Send and receive 10 messages sequentially works fine with txn")
        }
    ]