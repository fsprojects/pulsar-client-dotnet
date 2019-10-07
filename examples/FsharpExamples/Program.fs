// Learn more about F# at http://fsharp.org
open System
open System.Threading.Tasks

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging.Console
open Microsoft.Extensions.Logging
open System.Text


[<EntryPoint>]
let main argv =

    let serviceUrl = "pulsar://my-pulsar-cluster:31002";
    let subscriptionName = "my-subscription";
    let topicName = sprintf "my-topic-{%i}" DateTime.Now.Ticks;

    printfn "Example started"

    let loggerFactory =
        LoggerFactory.Create(fun builder ->
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole() |> ignore
        )


    PulsarClient.Logger <- loggerFactory.CreateLogger("PulsarLogger")

    let client =
        PulsarClientBuilder()
            .ServiceUrl(serviceUrl)
            .Build()

    let t = task {

        let! producer =
            ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync()

        let! consumer =
            ConsumerBuilder(client)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync()

        let! messageId = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Sent from F# at '%A'" DateTime.Now))
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.Payload |> Encoding.UTF8.GetString)

        do! consumer.AcknowledgeAsync(message.MessageId)
    }

    t.Wait()

    printfn "Example ended. Press any key to exit"
    Console.ReadKey() |> ignore

    0 // return an integer exit code
