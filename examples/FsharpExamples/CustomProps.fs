module CustomProps

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api

open System.Text
open Pulsar.Client.Common
open FSharp.UMX

let runCustomProps () =

    let serviceUrl = "pulsar://my-pulsar-cluster:30002"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks

    task {
        
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync()

        let! producer =
            client.NewProducer()
                .Topic(topicName)
                .CreateAsync()

        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync()

        let payload = Encoding.UTF8.GetBytes(sprintf "Sent from F# at '%A'" DateTime.Now)
        let msg = producer.NewMessage(payload, "F#", readOnlyDict [("1","one")])
        let! messageId = producer.SendAsync(msg)
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %s key: %s prop1: %s" (message.Data |> Encoding.UTF8.GetString) %message.Key message.Properties.["1"]

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
