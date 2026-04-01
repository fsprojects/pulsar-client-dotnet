module TlsAuthentication

// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text
open System.Security.Cryptography.X509Certificates

let runTlsAuthentication () =

    let serviceUrl = "pulsar+ssl://my-pulsar-cluster:6651"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;
    use ca = new X509Certificate2(@"path-to-ca.crt")
    let userTls = AuthenticationFactory.Tls(@"path-to-user.pfx")

    task {
        
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .EnableTls(true)
                .TlsTrustCertificate(ca)
                .Authentication(userTls)
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
        
        let! messageId = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Sent from F# at '%A'" DateTime.Now))
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.Data |> Encoding.UTF8.GetString)

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
