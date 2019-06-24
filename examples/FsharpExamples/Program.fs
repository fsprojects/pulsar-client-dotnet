// Learn more about F# at http://fsharp.org

open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive

[<EntryPoint>]
let main argv =
    printfn "Example started"

    let client =
        PulsarClientBuilder()
            .WithServiceUrl("pulsar://pulsar-broker:31002")
            .Build()
    let t = task {

        let! producer = 
            ProducerBuilder(client)
                .Topic("my-topic")
                .CreateAsync()  
        let! messageId = producer.SendAsync([||])
        printfn "%A" messageId

        let! consumer = 
            ConsumerBuilder(client)
                .Topic("my-topic")
                .SubscriptionName("my-subscription")
                .SubscribeAsync()  
        let! message = consumer.ReceiveAsync()
        printfn "%A" message
        do! consumer.AcknowledgeAsync(message)
    }
    t.Wait()
    
    printfn "Example ended"

    0 // return an integer exit code
