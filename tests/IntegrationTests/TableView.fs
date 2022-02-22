module Pulsar.Client.IntegrationTests.TableView

open System
open System.Linq
open Expecto
open Expecto.Flip
open Pulsar.Client.Api

open Serilog
open Pulsar.Client.IntegrationTests.Common

[<Tests>]
let tests =
    testList "TableView tests" [
        testTask "TableView with non-partition-topic works fine" {
            Log.Debug("Started testTableView")
            let client = getClient()
            let producerName = "tableViewProducer"
            let topicName = "public/default/" + Guid.NewGuid().ToString("N")

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync()

            do! producer.SendAsync(producer.NewMessage([| 1uy |], "key1"))
            do! producer.SendAsync(producer.NewMessage([| 2uy |], "key1"))
            do! producer.SendAsync(producer.NewMessage([| 3uy |], "key2"))
            do! producer.SendAsync(producer.NewMessage([| 4uy |]))

            let! (tableView : ITableView<byte[]>) =
                client.NewTableViewBuilder(Schema.BYTES())
                    .Topic(topicName)
                    .AutoUpdatePartitions(true)
                    .AutoUpdatePartitionsInterval(TimeSpan.FromSeconds(60.0))
                    .CreateAsync()

            Expect.equal "" 2 tableView.Count

            let keysCount = Enumerable.Count(tableView.Keys)
            Expect.equal "" 2 keysCount

            let valuesCount = Enumerable.Count(tableView.Values)
            Expect.equal "" 2 valuesCount
            
            let ran = Random()
            let contained = tableView.ContainsKey("key1")
            Expect.equal "" true contained
            
            let enumerator = tableView.GetEnumerator()
            Expect.isNotNull "" enumerator
            
            let value2 = tableView["key1"]
            let value3 = tableView["key2"]
            Expect.sequenceEqual "" [| 2uy |] value2
            Expect.sequenceEqual "" [| 3uy |] value3

            Log.Debug("Finished testTableView")
        }
    ]
