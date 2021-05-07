module Telemetry

open System
open System.Threading.Tasks
open OpenTelemetry
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System.Text
open Pulsar.Client.Otel.OTelProducerInterceptor
open Pulsar.Client.Otel.OtelConsumerInterceptor
open OpenTelemetry.Resources
open OpenTelemetry.Trace

let runTelemetry()=
    let serviceUrl = "pulsar://my-pulsar-cluster:32268"
    // let serviceUrl = "pulsar://localhost:6650"
    let subscriptionName = "my-subscription"
    let topicName = $"my-topic-{DateTime.Now.Ticks}"
    let producerSourceName = "pulsar.producer"
    let consumerSourceName = "pulsar.consumer"
    
    let _ = Sdk.CreateTracerProviderBuilder()
                .AddSource(producerSourceName, consumerSourceName)
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("telemetry"))
                .AddConsoleExporter().Build()
    let prodIntercept = OTelProducerInterceptor(producerSourceName, PulsarClient.Logger)
    let consIntercept = OTelConsumerInterceptor(consumerSourceName, PulsarClient.Logger)
    task {
        let! client =
            PulsarClientBuilder()                
                .ServiceUrl(serviceUrl)
                .BuildAsync()

        let! producer =
            client.NewProducer()
                .Intercept(prodIntercept)
                .Topic(topicName)
                .CreateAsync()

        let! consumer =
            client.NewConsumer()
                .Topic(topicName)
                .Intercept(consIntercept)
                .SubscriptionName(subscriptionName)
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync()        
        let! _ =
            [|
                producer.SendAsync(Encoding.UTF8.GetBytes("Sent 1 from F# at " + DateTime.Now.ToString()))
                producer.SendAsync(Encoding.UTF8.GetBytes("Sent 2 from F# at " + DateTime.Now.ToString()))
            |] |> Task.WhenAll |> Async.AwaitTask
       
        let! msgs =
            [|
                consumer.ReceiveAsync()
                consumer.ReceiveAsync()
            |] |> Task.WhenAll |> Async.AwaitTask
            
        for message in msgs do
            message.Data |> Encoding.UTF8.GetString |> Console.WriteLine
        do! consumer.AcknowledgeCumulativeAsync(msgs.[1].MessageId)
        
        (prodIntercept :> IProducerInterceptor<_>).Close()
        (consIntercept :> IConsumerInterceptor<_>).Close()
    }