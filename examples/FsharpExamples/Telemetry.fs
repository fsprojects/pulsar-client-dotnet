module Telemetry

open System
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
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks
   
    
    let _ = Sdk.CreateTracerProviderBuilder().
                                            AddSource(OTelProducerInterceptor<_>.Source, OTelConsumerInterceptor<_>.Source).
                                            SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("telemetry")).
                                            AddConsoleExporter().Build()
    let prodIntercept = OTelProducerInterceptor(PulsarClient.Logger)
    let consIntercept = OTelConsumerInterceptor(PulsarClient.Logger)
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
        
        let! _ = producer.SendAsync(Encoding.UTF8.GetBytes("Sent from F# at " + DateTime.Now.ToString()))
        let! message = consumer.ReceiveAsync()
        do! consumer.AcknowledgeAsync(message.MessageId)
    }