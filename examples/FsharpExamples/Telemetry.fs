module Telemetry

open System
open System.Threading.Tasks
open OpenTelemetry
open Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text
open Pulsar.Client.Otel.OTelProducerInterceptor
open Pulsar.Client.Otel.OtelConsumerInterceptor
open OpenTelemetry.Resources
open OpenTelemetry.Trace

let runTelemetry()=
    let serviceUrl = "pulsar://localhost:6650"
    let subscriptionName = "my-subscription"
    let topicName = $"my-topic-{DateTime.Now.Ticks}"
    let producerSourceName = "pulsar.producer"
    let consumerSourceName = "pulsar.consumer"
    
    Sdk.CreateTracerProviderBuilder()
                .AddSource(producerSourceName, consumerSourceName)
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("telemetry"))
                .AddConsoleExporter().Build() |> ignore
    task {
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync()
        use prodIntercept = new OTelProducerInterceptor<byte[]>(producerSourceName, PulsarClient.Logger)
        use consIntercept = new OTelConsumerInterceptor<byte[]>(consumerSourceName, PulsarClient.Logger)

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
            |] |> Task.WhenAll
       
        let! msgs =
            [|
                consumer.ReceiveAsync()
                consumer.ReceiveAsync()
            |] |> Task.WhenAll
            
        for message in msgs do
            message.Data |> Encoding.UTF8.GetString |> Console.WriteLine
        do! consumer.AcknowledgeCumulativeAsync(msgs.[1].MessageId)
    }