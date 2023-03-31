module RealWorld

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api

open Microsoft.Extensions.Logging
open Pulsar.Client.Common
open System.Text
open System.Threading
open System.Threading.Tasks



let internal processMessages<'a> (consumer: IConsumer<byte[]>, logger: ILogger, f: Message<byte[]> -> Task<unit>, ct: CancellationToken) =
    task {
        try
            while not ct.IsCancellationRequested do
                let mutable success = false
                let! message = consumer.ReceiveAsync(ct)
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
            | :? TaskCanceledException ->
                logger.LogInformation("ProcessMessages cancelled for {0}", consumer.Topic)
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
                .EnableBatching(false)
                .CreateAsync()

        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync()

        use cts = new CancellationTokenSource()
        let token = cts.Token
        Task.Run(fun () ->
            task {
                do! processMessages(consumer, logger, (fun (message) ->
                                    let messageText = Encoding.UTF8.GetString(message.Data)
                                    logger.LogInformation("Received: {0}", messageText)
                                    Task.FromResult()), token)
            }, cts.Token) |> ignore

        for i in 1..100 do
            do! sendMessage(producer, logger, Encoding.UTF8.GetBytes(sprintf "Sent from C# at %A message %i" DateTime.Now i))

        cts.Cancel()
        do! Task.Delay(200);// wait for pending acknowledgments to complete
        do! consumer.DisposeAsync()
        do! producer.DisposeAsync()
        do! client.CloseAsync()
    }
