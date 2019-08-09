// Learn more about F# at http://fsharp.org

open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging.Console
open Microsoft.Extensions.Logging
open System.Text


[<EntryPoint>]
let main argv =
    printfn "Example started"

    PulsarClient.Logger <- ConsoleLogger("PulsarLogger", Func<string,LogLevel,bool>(fun x y -> true), true)
    let client =
        PulsarClientBuilder()
            .WithServiceUrl("pulsar://my-pulsar-cluster:31002")
            .Build()
    let t = task {

        let! producer =
            ProducerBuilder(client)
                .Topic("my-topic")
                .CreateAsync()
        let! messageId = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes("Sent from F#"))
        printfn "%A" messageId

        let! consumer =
            ConsumerBuilder(client)
                .Topic("my-topic")
                .SubscriptionName("my-subscription")
                .SubscribeAsync()

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.Payload |> Encoding.UTF8.GetString)
        do! consumer.AcknowledgeAsync(message.MessageId)
    }
    t.Wait()

    printfn "Example ended"

    0 // return an integer exit code
