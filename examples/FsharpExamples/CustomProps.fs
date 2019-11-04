module CustomProps

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open Pulsar.Client.Common
open FSharp.UMX

let runCustomProps () =

    let serviceUrl = "pulsar://my-pulsar-cluster:31002"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks

    let client =
        PulsarClientBuilder()
            .ServiceUrl(serviceUrl)
            .Build()

    task {

        let! producer =
            ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync()

        let! consumer =
            ConsumerBuilder(client)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync()

        let payload = Encoding.UTF8.GetBytes(sprintf "Sent from F# at '%A'" DateTime.Now)
        let! messageId = producer.SendAsync(MessageBuilder(payload, "F#", dict [("1","one")]))
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %s key: %s prop1: %s" (message.Payload |> Encoding.UTF8.GetString) %message.MessageKey message.Properties.["1"]

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
