module OauthTest.runOauth

open System
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System.Text


let runOauth() =    
    
    let privateKeyFileName = "credentials_file.json"    
    let startup = System.IO.Path.GetDirectoryName (System.Reflection.Assembly.GetExecutingAssembly().Location)
    let startupWithCreds = System.IO.Path.Combine (startup, privateKeyFileName)    
    let fileUri = Uri(startupWithCreds)
    
    let issuerUrl = Uri("https://pulsar-sample.us.auth0.com")
    let audience = Uri("https://pulsar-sample.us.auth0.com/api/v2/")
    
    let serviceUrl = "pulsar://localhost:6650"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;
    task{
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Authentication(AuthenticationFactory.oauth2(issuerUrl,fileUri,audience))
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
        
        let! messageId = producer.SendAsync(Encoding.UTF8.GetBytes("Sent from F# at " + DateTime.Now.ToString()))
        printfn "MessageId is: '%A'" messageId

        let! message = consumer.ReceiveAsync()
        printfn "Received: %A" (message.Data |> Encoding.UTF8.GetString)

        do! consumer.AcknowledgeAsync(message.MessageId)
    }
    