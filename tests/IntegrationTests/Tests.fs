module Tests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks


[<Literal>]
let pulsarAddress = "pulsar://my-pulsar-cluster:31002"

[<Tests>]
let tests =

    testList "basic" [
        testCase "Send and receive 100 messages works fine" <| fun () ->
            let client =
                PulsarClientBuilder()
                    .WithServiceUrl(pulsarAddress)
                    .Build()
            let topicName = "test-topic" //"topic-" + DateTimeOffset.UtcNow.UtcTicks.ToString()

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let! producer =
                            ProducerBuilder(client)
                                .Topic(topicName)
                                .CreateAsync()

                        for i in [1..100] do
                            let! _ = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes("Message #" + string i))
                            ()
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let! consumer =
                            ConsumerBuilder(client)
                                .Topic(topicName)
                                .SubscriptionName("test-subscription")
                                .SubscribeAsync()

                        for i in [1..100] do
                            let! message = consumer.ReceiveAsync()
                            let received = Encoding.UTF8.GetString(message.Payload)
                            Expect.equal "" ("Message #" + string i) received
                            do! consumer.AcknowledgeAsync(message)
                            ()
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)
    ]
