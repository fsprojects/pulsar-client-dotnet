module Pulsar.Client.IntegrationTests.Common

open System
open System.Diagnostics
open System.Net.Http
open Pulsar.Client.Api

open System.Text
open System.Text.Json
open System.Threading.Tasks
open System.Security.Cryptography.X509Certificates
open Pulsar.Client.Common
open Pulsar.Client.Transaction
open Serilog
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open FSharp.UMX


[<Literal>]
let pulsarAddress = "pulsar://127.0.0.1:6650"

[<Literal>]
let pulsarHttpAddress = "http://127.0.0.1:8080"

#if !NOTLS
[<Literal>]
let pulsarSslAddress = "pulsar+ssl://127.0.0.1:6651"

// ssl folder copied by from https://github.com/apache/pulsar/tree/master/tests/docker-images/latest-version-image/ssl
// generate pfx file from pem, leave the password blank
// openssl pkcs12 -in admin.cert.pem -inkey admin.key-pk8.pem -export -out admin.pfx
let ca = X509CertificateLoader.LoadCertificateFromFile(@"../ssl/ca.cert.pem")
// Load the client certificate for the mtls encryption
// The admin.pfx contains both client certificate and private key, which can be used for both mtls encryption and authentication.
let clientCert = X509CertificateLoader.LoadPkcs12FromFile(@"../ssl/admin.pfx", "", X509KeyStorageFlags.DefaultKeySet, Pkcs12LoaderLimits.Defaults)
// Use client certificate for tls authentication
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

let commonHttpLookupClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarHttpAddress)
        .BuildAsync().Result

let commonHttpClient = new HttpClient()

let getClient() = commonClient

let getHttpLookupClient() = commonHttpLookupClient

let extractTimeStamp (date: DateTime) : TimeStamp =
    let mss = (date - DateTime.UnixEpoch).TotalMilliseconds |> int64
    %mss

#if !NOTLS
let sslClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .TlsTrustCertificate(ca)
        .BuildAsync().Result

let sslAdminClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .TlsTrustCertificate(ca)
        .Authentication(sslAdmin)
        .BuildAsync().Result

let sslTokenClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .TlsTrustCertificate(ca)
        .TlsCertificate(clientCert)
        .Authentication(AuthenticationFactory.Token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.hkExtZmCXOi1byo_sVzZrxZcwN88tvVVMlNcxQEs3TY"))
        .BuildAsync().Result

let sslUser1Client =
    PulsarClientBuilder()
        .ServiceUrl(pulsarSslAddress)
        .TlsTrustCertificate(ca)
        .Authentication(sslUser1)
        .BuildAsync().Result

let getSslClient() = sslClient

let getSslAdminClient() = sslAdminClient

let getSslTokenClient() = sslTokenClient

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

let createPartitionedTopic (topicName: string) (numPartitions: int) =
    task {
        use http = new HttpClient(new HttpClientHandler(UseProxy = false))
        http.BaseAddress <- Uri(pulsarHttpAddress)
        http.Timeout <- TimeSpan.FromSeconds(15.0)
        use partitions = new StringContent(string numPartitions, Encoding.UTF8, "application/json")
        let! created = http.PutAsync($"/admin/v2/persistent/{topicName}/partitions", partitions)
        created.EnsureSuccessStatusCode() |> ignore
    }

// msgOutCounter counts the messages the broker has sent to the subscription's consumers
let waitForDispatchedMessages (topicName: string) (subscriptionName: string) (count: int64) (timeout: TimeSpan) =
    task {
        use http = new HttpClient(new HttpClientHandler(UseProxy = false))
        http.BaseAddress <- Uri(pulsarHttpAddress)
        http.Timeout <- TimeSpan.FromSeconds(15.0)
        let sw = Stopwatch.StartNew()
        let mutable dispatched = 0L
        while dispatched < count do
            if sw.Elapsed > timeout then
                failwith $"The broker dispatched {dispatched}/{count} messages of {topicName} to {subscriptionName} in {timeout}"
            let! stats = http.GetStringAsync($"/admin/v2/persistent/{topicName}/stats")
            use json = JsonDocument.Parse(stats)
            dispatched <- json.RootElement.GetProperty("subscriptions").GetProperty(subscriptionName).GetProperty("msgOutCounter").GetInt64()
            if dispatched < count then
                do! Task.Delay(50)
    }

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

let unwrap (ex: exn) =
    match ex with
    | :? AggregateException as agg -> agg.GetBaseException()
    | _ -> ex

let assertNotConnected (work: Task<'T>) = task {
    try
        let! _ = work.WaitAsync(TimeSpan.FromSeconds(5.0))
        failwith "Task completed successfully after mailbox failure"
    with ex ->
        match unwrap ex with
        | :? NotConnectedException -> ()
        | :? System.TimeoutException -> failwith "Task did not fault within 5 seconds"
        | other -> failwith $"Expected NotConnectedException but got {other.GetType().Name}: {other.Message}"
}

// The failure has to keep its cause, so the innermost exception is the one that stopped the consumer.
let assertNotConnectedBecauseOf (cause: Type) (work: Task<'T>) = task {
    let mutable error = null
    try
        let! _ = work.WaitAsync(TimeSpan.FromSeconds(5.0))
        ()
    with ex ->
        error <- ex
    match error with
    | null -> failwith "Task completed successfully after mailbox failure"
    | ex ->
        match unwrap ex with
        | :? NotConnectedException as notConnected when cause.IsInstanceOfType(notConnected.GetBaseException()) -> ()
        | :? NotConnectedException as notConnected ->
            failwith $"Expected the failure to be caused by {cause.Name} but got {notConnected.GetBaseException()}"
        | :? System.TimeoutException -> failwith "Task did not fault within 5 seconds"
        | other -> failwith $"Expected NotConnectedException but got {other.GetType().Name}: {other.Message}"
}

// Retries until the condition holds. Internal state read from the test thread can change underneath it,
// so a failing read is retried too. A condition that doesn't complete before the deadline counts as false.
let waitUntil (description: string) (condition: unit -> Task<bool>) = task {
    let deadline = DateTime.UtcNow.AddSeconds 15.0
    let mutable satisfied = false
    while not satisfied do
        let remaining = max TimeSpan.Zero (deadline - DateTime.UtcNow)
        let! result = task {
            try
                return! condition().WaitAsync(remaining)
            with _ ->
                return false
        }
        if result then
            satisfied <- true
        elif DateTime.UtcNow >= deadline then
            failwith $"Timed out waiting until {description}"
        else
            do! Task.Delay 100
}
