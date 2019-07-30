module Tests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Serilog.Sinks.SystemConsole
open Serilog.Configuration
open Microsoft.Extensions.Logging.Console
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open Serilog.Core


[<Literal>]
let pulsarAddress = "pulsar://my-pulsar-cluster:31002"

let configureLogging() =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console(theme = AnsiConsoleTheme.Code, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3} {ThreadId}] {Message:lj}{NewLine}{Exception}")
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .CreateLogger()
    let serviceCollection = new ServiceCollection()
    let sp =
        serviceCollection
            .AddLogging(fun configure -> configure.AddSerilog(dispose = true) |> ignore)
            .BuildServiceProvider()
    let logger = sp.GetService<ILogger<PulsarClient>>()
    PulsarClient.Logger <- logger

[<Tests>]
let tests =

    configureLogging()

    let getClient() =
        PulsarClientBuilder()
            .WithServiceUrl(pulsarAddress)
            .Build()

    let produceMessages (producer: Producer) number producerName =
        task {
            for i in [1..number] do
                let! _ = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %A" i producerName (DateTime.Now.ToLongTimeString()) ))
                ()
        }

    let consumeMessages (consumer: Consumer) number consumerName =
        task {
            for i in [1..number] do
                let! message = consumer.ReceiveAsync()
                let received = Encoding.UTF8.GetString(message.Payload)
                do! consumer.AcknowledgeAsync(message)
                Log.Debug("{0} received {1}", consumerName, received)
                let expected = "Message #" + string i
                if received.StartsWith(expected) |> not then
                    failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
        }

    testList "basic" [
        testCase "Send and receive 100 messages concurrently works fine in default configuration" <| fun () ->

            Log.Debug("Started Send and receive 100 messages concurrently works fine in default configuration")
            let client = getClient()
            let topicName = "public/default/test"

            let producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName("concurrent")
                    .CreateAsync()
                    .Result

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
                    .Result

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer 100 "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer 100 "concurrent"
                    }:> Task)

            Task.WaitAll(producerTask, consumerTask)

            Log.Debug("Finished Send and receive 100 messages concurrently works fine in default configuration")

        testCase "Send 100 messages and then receiving them works fine when retention is set on namespace" <| fun () ->

            Log.Debug("Started send 100 messages and then receiving them works fine when retention is set on namespace")
            let client = getClient()
            let topicName = "public/retention/topic-" + DateTimeOffset.UtcNow.UtcTicks.ToString()

            let producer =
                ProducerBuilder(client)
                    .ProducerName("sequential")
                    .Topic(topicName)
                    .CreateAsync()
                    .Result

            (produceMessages producer 100 "sequential").Wait()

            let consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("sequential")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync()
                    .Result

            (consumeMessages consumer 100 "sequential").Wait()
            Log.Debug("Finished send 100 messages and then receiving them works fine when retention is set on namespace")
    ]
