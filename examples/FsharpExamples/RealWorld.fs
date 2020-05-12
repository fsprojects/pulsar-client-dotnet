module RealWorld

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging
open Pulsar.Client.Common
open System.Text
open System.Threading
open System.Threading.Tasks



let internal processMessages<'a> (consumer: IConsumer<byte[]>, logger: ILogger, f: Message<byte[]> -> Task<unit>, cancellationToken: CancellationToken) =
    task {
        try
            while not cancellationToken.IsCancellationRequested do
                let mutable success = false
                let! message = consumer.ReceiveAsync()
                try
                    do! f message
                    success <- true
                    logger.LogDebug("Message handled")
                with
                | ex ->
                    logger.LogError(ex, "Can't process message {0}, MessageId={1}", consumer.Topic, message.MessageId)
                    do! consumer.NegativeAcknowledge(message.MessageId)
                if success then
                    do! consumer.AcknowledgeAsync(message.MessageId)
                    logger.LogDebug("Message acknowledged")
        with
            | ex ->
                logger.LogError(ex, "ProcessMessages failed for {0}", consumer.Topic)
   }
    
let internal sendMessage (producer: IProducer<byte[]>, logger: ILogger, message: byte[]) =
    task {
        try
            let! messageId = producer.SendAsync(message)
            logger.LogDebug("MessageSent to {0}. MessageId={1}", producer.Topic, messageId)
        with ex ->
            logger.LogError(ex, "Error on produce message to {0}", producer.Topic)
    }

let runRealWorld (logger: ILogger) =

    let serviceUrl = "pulsar://my-pulsar-cluster:30002"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;

    let client =
        PulsarClientBuilder()
            .ServiceUrl(serviceUrl)
            .Build()

    task {

        let! producer =
            client.NewProducer()
                .Topic(topicName)
                .EnableBatching(false)
                .CreateAsync()

        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync()
        
        let cts = new CancellationTokenSource()
        let token = cts.Token
        Task.Run(fun () ->
            task {
                do! processMessages(consumer, logger, (fun (message) ->        
                                    let messageText = Encoding.UTF8.GetString(message.Data)
                                    logger.LogInformation("Received: {0}", messageText)
                                    Task.FromResult()), token)
            } :> Task) |> ignore

        for i in 1..100 do
            do! sendMessage(producer, logger, Encoding.UTF8.GetBytes(sprintf "Sent from C# at %A message %i" DateTime.Now i))
        
        cts.Dispose()
        do! Task.Delay(200);// wait for pending acknowledgments to complete
        do! consumer.DisposeAsync()
        do! producer.DisposeAsync()
        do! client.CloseAsync()
    }
