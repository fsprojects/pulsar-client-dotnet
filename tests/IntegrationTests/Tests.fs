module Tests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Microsoft.Extensions.Logging.Console
open Microsoft.Extensions.Logging


[<Literal>]
let pulsarAddress = "pulsar://my-pulsar-cluster:31002"

[<Tests>]
let tests =

    //PulsarClient.Logger <- ConsoleLogger("PulsarLogger", Func<string,LogLevel,bool>(fun x y -> y >= LogLevel.Warning), true)

    let getClient() =
        PulsarClientBuilder()
            .WithServiceUrl(pulsarAddress)
            .Build()

    let produceMessages (producer: Producer) number topicName =
        task {
            for i in [1..number] do
                let! _ = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes("Message #" + string i))
                ()
        }

    let consumeMessages (consumer: Consumer) number topicName =
        task {
            for i in [1..number] do
                let! message = consumer.ReceiveAsync()
                let received = Encoding.UTF8.GetString(message.Payload)
                Expect.equal "" ("Message #" + string i) received
                do! consumer.AcknowledgeAsync(message)
                ()
        }

    testList "basic" [
        testCase "Send and receive 100 messages concurrently works fine in default configuration" <| fun () ->
            let client = getClient()
            let topicName = "public/default/topic-" + DateTimeOffset.UtcNow.UtcTicks.ToString()

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
                    .Result

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 topicName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 topicName
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

        testCase "Send 100 messages and then receiving them works fine when retention is set on namespace " <| fun () ->
            let client = getClient()
            let topicName = "public/retention/topic-" + DateTimeOffset.UtcNow.UtcTicks.ToString()

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            (produceMessages producer 100 topicName).Wait()

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync()
                    .Result

            (consumeMessages consumer 100 topicName).Wait()
    ]
