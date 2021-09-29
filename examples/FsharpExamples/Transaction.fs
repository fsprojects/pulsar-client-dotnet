module Transaction

// Learn more about F# at http://fsharp.org
open System
open System
open Avro
open Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text

let runTransaction () =

    let serviceUrl = "pulsar://my-pulsar-cluster:31004"
    let subscriptionName = "my-subscription"
    let topicName1 = sprintf "my-topic-%i-1" DateTime.Now.Ticks;
    let topicName2 = sprintf "my-topic-%i-2" DateTime.Now.Ticks;


    task {
        
        let! client =
            PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .EnableTransaction(true)
                .BuildAsync()

        let! producer =
            client.NewProducer(Schema.STRING())
                .Topic(topicName1)
                .CreateAsync()
                
        let! consumer =
            client.NewConsumer(Schema.STRING())
                .Topic(topicName2)
                .SubscriptionName(subscriptionName)
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync()

        let! transConsumer =
            client.NewConsumer(Schema.STRING())
                .Topic(topicName1)
                .SubscriptionName(subscriptionName)
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync()
                
        let! transProducer =
            client.NewProducer(Schema.STRING())
                .Topic(topicName2)
                .SendTimeout(TimeSpan.Zero)
                .CreateAsync()
        
        let message1 = producer.NewMessage("Sent from F# at " + DateTime.Now.ToString())
        let! messageId1 = producer.SendAsync(message1)
        printfn "Message was initially sent with MessageId=%s" <| messageId1.ToString()
        
        let! transaction =
            client.NewTransaction()
                .BuildAsync()

        let! message2 = transConsumer.ReceiveAsync()
        let text1 = message2.GetValue()
        printfn "Message received in transaction: %s" text1
        do! transConsumer.AcknowledgeAsync(message2.MessageId, transaction);
        
        let message3 = transProducer.NewMessage(text1, txn = transaction)
        let! messageId3 = transProducer.SendAsync(message3)
        printfn "Message was produced in transaction, but not yet commited with MessageId=%s" <| messageId3.ToString()
        
        do! transaction.Commit()

        let! message4 = consumer.ReceiveAsync()
        let text2 = message4.GetValue()
        printfn "Message received normally: %s" text2
        do! consumer.AcknowledgeAsync(message4.MessageId)
        
    }
