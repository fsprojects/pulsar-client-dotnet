module Schema

// Learn more about F# at http://fsharp.org
open System
open System.ComponentModel.DataAnnotations
open Pulsar.Client.Api

open Pulsar.Client.Common

[<CLIMutable>]
type Product = {
    [<Required>]
    Name: string
    Rating: float
}

let runSchema () =

    let serviceUrl = "pulsar://my-pulsar-cluster:30002"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;

    task {
        
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync()

        let! producer =
            client.NewProducer(Schema.JSON<Product>())
                .Topic(topicName)
                .CreateAsync()

        let! consumer =
            client.NewConsumer(Schema.JSON<Product>())
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync()
        
        let! messageId = producer.SendAsync({ Name = "IPhone"; Rating = 1.1 })
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.GetValue())

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
