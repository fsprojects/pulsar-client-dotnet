// Learn more about F# at http://fsharp.org

open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextSensitive

[<EntryPoint>]
let main argv =
    printfn "Example started"

    let client =
        PulsarClientBuilder()
            .WithServiceUrl("pulsar://pulsar-broker:31002")
            .Build()
    task {
        let! consumer = 
            ConsumerBuilder(client)
                .Topic("my-topic")
                .SubscriptionName("my-subscription")
                .SubscribeAsync()  
        let! message = consumer.ReceiveAsync()
        printfn "%A" message
        do! consumer.AcknowledgeAsync(message)
    } |> ignore  
    
    printfn "Example ended"

    0 // return an integer exit code
