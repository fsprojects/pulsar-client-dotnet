module Pulsar.Client.IntegrationTests.Common

open System
open System.Net.Http
open Pulsar.Client.Api

open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Transaction
open Serilog
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open FSharp.UMX


[<Literal>]
let pulsarAddress = "pulsar://127.0.0.1:6650"

#if !NOTLS
[<Literal>]
let pulsarSslAddress = "pulsar+ssl://127.0.0.1:6651"

[<Literal>]
let pulsarHttpAddress = "http://127.0.0.1:8080"

// ssl folder copied by from https://github.com/apache/pulsar/tree/master/tests/docker-images/latest-version-image/ssl
// generate pfx file from pem, leave the password blank
// openssl pkcs12 -in admin.cert.pem -inkey admin.key-pk8.pem -export -out admin.pfx
let ca = new Security.Cryptography.X509Certificates.X509Certificate2(@"../ssl/ca.cert.pem")
let sslAdmin = AuthenticationFactory.Tls(@"../ssl/admin.pfx")
let sslUser1 = AuthenticationFactory.Tls(@"../ssl/user1.pfx")
#endif

let configureLogging() =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Warning()
            .WriteTo.Console(theme = AnsiConsoleTheme.Code, outputTemplate="[{Timestamp:HH:mm:ss.fff} {Level:u3} {ThreadId}] {Message:lj}{NewLine}{Exception}")
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .CreateLogger()
    let serviceCollection = ServiceCollection()
    let sp =
        serviceCollection
            .AddLogging(fun configure -> configure.AddSerilog(dispose = true) |> ignore)
            .BuildServiceProvider()
    let logger = sp.GetService<ILogger<PulsarClient>>()
    PulsarClient.Logger <- logger

let commonClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .BuildAsync().Result

let commonHttpClient = new HttpClient()

let getClient() = commonClient

let extractTimeStamp (date: DateTime) : TimeStamp =
    let mss = (date - DateTime.UnixEpoch).TotalMilliseconds |> int64
    %mss

#if !NOTLS
let sslClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .EnableTls(true)
        .TlsTrustCertificate(ca)
        .BuildAsync().Result

let sslAdminClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .EnableTls(true)
        .TlsTrustCertificate(ca)
        .Authentication(sslAdmin)
        .BuildAsync().Result

let sslUser1Client =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .EnableTls(true)
        .TlsTrustCertificate(ca)
        .Authentication(sslUser1)
        .BuildAsync().Result

let getSslClient() = sslClient

let getSslAdminClient() = sslAdminClient

let getSslUser1Client() = sslUser1Client
#endif

let getNewClient() =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .BuildAsync().Result

let getStatsClient() =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .StatsInterval(TimeSpan.FromSeconds 1.0)
        .BuildAsync().Result

let getTxnClient() =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .EnableTransaction(true)
        .BuildAsync().Result

let produceMessages (producer: IProducer<byte[]>) number producerName =
    task {
        for i in 1..number do
            let! _ = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
            ()
    }

let produceMessagesWithProps (producer: IProducer<byte[]>) number producerName =
    task {
        for i in 1..number do
            let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) )
            let key = i.ToString()
            let props = readOnlyDict [("prop1",key);("prop2",key)]
            let! _ = producer.NewMessage(payload, key, props) |> producer.SendAsync
            ()
    }

let produceMessagesWithSameKey (producer: IProducer<byte[]>) number key producerName =
    task {
        for i in 1..number do
            let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) )
            let! _ = producer.NewMessage(payload, key) |> producer.SendAsync
            ()
    }

let produceMessagesWithTxn (producer: IProducer<byte[]>) (txn: Transaction) number producerName =
    task {
        for i in 1..number do
            let data = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()))
            let message = producer.NewMessage(data, txn = txn)
            let! _ = producer.SendAsync(message)
            ()
    }

let generateMessages number producerName =
    [|
        for i in 1..number do
            yield sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString())
    |]

let producePredefinedMessages (producer: IProducer<byte[]>) (messages: string[]) =
    task {
        for msg in messages do
            let! _ = producer.SendAsync(Encoding.UTF8.GetBytes(msg))
            ()
    }

let fastProduceMessages (producer: IProducer<byte[]>) number producerName =
    task {
        for i in 1..number do
            let! _ = producer.SendAndForgetAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
            ()
    }

let fastProduceMessagesWithSameKey (producer: IProducer<byte[]>) number key producerName =
    task {
        for i in 1..number do
            let payload = Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) )
            let! _ = producer.NewMessage(payload, key) |> producer.SendAndForgetAsync
            ()
    }

let createSendAndWaitTasks (producer: IProducer<byte[]>) number producerName =
    let createTask taskNumber =
        let message = sprintf "Message #%i Sent from %s on %s" taskNumber producerName (DateTime.Now.ToLongTimeString())
        let messageBytes = Encoding.UTF8.GetBytes(message)
        let task = Task.Run(fun() -> producer.SendAsync(messageBytes) |> ignore)
        (task, message)

    [|1..number|] |> Array.map createTask

let getMessageNumber (msg: string) =
    let ind1 = msg.IndexOf("#")
    let ind2 = msg.IndexOf("Sent")
    let subString = msg.Substring(ind1+1, ind2 - ind1 - 2)
    int subString

let consumeMessages (consumer: IConsumer<byte[]>) number consumerName =
    task {
        for i in 1..number do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Data)
            Log.Debug("{0} received {1}", consumerName, received)
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} acknowledged {1}", consumerName, received)
            let expected = "Message #" + string i
            if received.StartsWith(expected) |> not then
                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
    }

let consumeMessagesWithProps (consumer: IConsumer<byte[]>) number consumerName =
    task {
        for i in 1..number do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Data)
            Log.Debug("{0} received {1}", consumerName, received)
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} acknowledged {1}", consumerName, received)
            let expected = "Message #" + string i
            if received.StartsWith(expected) |> not then
                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
            if message.Key <> %(i.ToString()) then
                failwith <| sprintf "Incorrect message key expected %i received %s consumer %s" i %message.Key consumerName
            if (message.Properties.Count = 2
                && message.Properties.["prop1"] = i.ToString()
                && message.Properties.["prop2"] = i.ToString()) |> not then
                failwith <| sprintf "Incorrect properties %s" consumerName
    }

let consumeMessagesWithTxn (consumer: IConsumer<byte[]>) (txn: Transaction) number consumerName =
    task {
        for i in 1..number do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Data)
            Log.Debug("{0} received {1}", consumerName, received)
            do! consumer.AcknowledgeAsync(message.MessageId, txn)
            Log.Debug("{0} acknowledged {1}", consumerName, received)
            let expected = "Message #" + string i
            if received.StartsWith(expected) |> not then
                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
    }

let consumeAndVerifyMessages (consumer: IConsumer<byte[]>) consumerName (expectedMessages : string[]) =
    task {
        for _ in [1..expectedMessages.Length] do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Data)
            Log.Debug("{0} received {1}", consumerName, received)
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} acknowledged {1}", consumerName, received)
            if expectedMessages |> Array.contains received |> not then
                failwith <| sprintf "Received unexpected message '%s' consumer %s" received consumerName
    }
