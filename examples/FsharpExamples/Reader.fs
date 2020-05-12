module ReaderApi

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text

let runReader () =

    let serviceUrl = "pulsar://my-pulsar-cluster:30002"

    // retention should be set on topic so messages won't disappear
    let topicName = sprintf "public/retention/my-topic-%i" DateTime.Now.Ticks

    let client =
        PulsarClientBuilder()
            .ServiceUrl(serviceUrl)
            .Build()

    task {

        let! producer =
            client.NewProducer()
                .Topic(topicName)
                .CreateAsync()

        let! messageId1 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Sent 1 from F# at '%A'" DateTime.Now))
        printfn "MessageId is: '%A'" messageId1
        let! messageId2 = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Sent 2 from F# at '%A'" DateTime.Now))
        printfn "MessageId is: '%A'" messageId2

        let! reader =
            client.NewReader()
                .Topic(topicName)
                .StartMessageId(messageId1)
                .CreateAsync()

        let! message = reader.ReadNextAsync()
        printfn "Received: %A" (message.Data |> Encoding.UTF8.GetString)
    }
