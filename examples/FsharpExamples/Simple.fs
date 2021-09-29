module Simple

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text

let runSimple () =

    let serviceUrl = "pulsar://127.0.0.1:6650"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;


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
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync()        
        
        let! messageId = producer.SendAsync(Encoding.UTF8.GetBytes("Sent from F# at " + DateTime.Now.ToString()))
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.Data |> Encoding.UTF8.GetString)

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
