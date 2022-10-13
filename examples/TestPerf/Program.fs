// Learn more about F# at http://fsharp.org
open System
open System.Diagnostics
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Api
open Microsoft.Extensions.Logging
open Pulsar.Client.Common

let loggerFactory =
    LoggerFactory.Create(fun builder ->
        builder
            .SetMinimumLevel(LogLevel.Information)
            .AddConsole() |> ignore
    )
PulsarClient.Logger <- loggerFactory.CreateLogger("PulsarLogger")

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

        let data = Encoding.UTF8.GetBytes("Sent from F# at " + DateTime.Now.ToString())

        let sw = Stopwatch.StartNew()
        let size = 5000000

        let task1 =
            task {
                for i in 1..size do
                    let! _ = producer.SendAndForgetAsync(data)
                    ()
            }


        let task2 =
            task {
                for i in 1..size do
                    let! message = consumer.ReceiveAsync()
                    if (i%100000 = 0) then
                        printfn "Received %i messages in %i ms" i sw.ElapsedMilliseconds
                    do! consumer.AcknowledgeAsync(message.MessageId)
            }

        let! _ = Task.WhenAll(task1, task2)

        Console.WriteLine("Processed {0} messages. Elapsed: {1}s", size, sw.Elapsed.TotalSeconds)

    }

runSimple().Wait()
