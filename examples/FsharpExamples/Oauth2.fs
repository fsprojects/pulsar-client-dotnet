module Oauth2

open System
open Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text

let configFilePath() =
    let configFolderName = "Oauth2Files"
    let privateKeyFileName = "credentials_file.json"
    let startup = System.IO.Path.GetDirectoryName (System.Reflection.Assembly.GetExecutingAssembly().Location)
    let indexOfConfigDir = startup.IndexOf "examples"
    let examplesFolder = startup.Substring(0, startup.Length - indexOfConfigDir - 3)
    let configFolder = System.IO.Path.Combine(examplesFolder,configFolderName)
    let ret = System.IO.Path.Combine (configFolder, privateKeyFileName)
    if (System.IO.File.Exists(ret))
    then ret
    else raise (System.IO.FileNotFoundException("can't find credentials file"))
        
//In order to run this example one has to have authentication on broker
//Check configuration files to see how to set up authentication in broker
//In this example Auth0 server is used, look at it's response in Auth0response file  
let runOauth() =     
      
    let fileUri = Uri(configFilePath())
    let issuerUrl = Uri("https://pulsar-sample.us.auth0.com")
    let audience = "https://pulsar-sample.us.auth0.com/api/v2/"
    
    let serviceUrl = "pulsar://localhost:6650"
    let subscriptionName = "my-subscription"
    let topicName = sprintf "my-topic-%i" DateTime.Now.Ticks;
    task{
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Authentication(AuthenticationFactoryOAuth2.ClientCredentials(issuerUrl, audience, fileUri))
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
    