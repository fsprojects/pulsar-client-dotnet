module Pulsar.Client.IntegrationTests.MessageCrypto

open System
open System.IO
open System.Text
open System.Text.Json
open Expecto
open ProtoBuf

open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Crypto
open Pulsar.Client.IntegrationTests.Common
open Serilog

let publicKeys =
    Map.empty.
        Add("Rsa1024key1", @"-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC/xmFrob/LU4xDHpQZR1Yk8PuJ
dZUPZFuZZIsS+IfZD2TrEHIG2ie0Mof05yDor9cJqy8TmfxMggY/KQNaCLW+Acpm
znS+7uQ2FD9AXZ5beyv+wcGAGGFsGFDOluepoe1ljTmtb1rjxY69R+AiNAJdr0KM
mHymC9eqBKD1t1i86wIDAQAB
-----END PUBLIC KEY-----").

        Add("Rsa1024key2", @"-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC59k14/QlPcOgvvl7rujdm2RFi
Mgc1cwbZ5yUGYBrGFd6eVnvH16Q7igyde5e0WytEYpeabB1KcRVgDSkElNkgs9Ns
7UcYaKbnRqHRgPjMdyu+X70mHOeja2iMjPxR+hrnZFrNM2x0MevHK26WORUPFHIE
/2GVt86a3z0d8xj0qQIDAQAB
-----END PUBLIC KEY-----").

        Add("Rsa1024key3", @"-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC5z1YTpTdHRy0bLtaklkMiXGjk
8BTaB644Bpqq6sA9TV/IQq29qHBmQexPK4uYbToKxmul5JkupuEQ5ACAIo2MO17y
S2xdqMyIsWeLKvbVxKcOPUiV1s5SD2FMkcZNc+iQLxZRdqs5QOBxmwKjSKBJ2owR
JMMHhylIlH9EMaN84QIDAQAB
-----END PUBLIC KEY-----")


let privateKeysConsumer1 =
    Map.empty.
        Add("Rsa1024key1", @"-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQC/xmFrob/LU4xDHpQZR1Yk8PuJdZUPZFuZZIsS+IfZD2TrEHIG
2ie0Mof05yDor9cJqy8TmfxMggY/KQNaCLW+AcpmznS+7uQ2FD9AXZ5beyv+wcGA
GGFsGFDOluepoe1ljTmtb1rjxY69R+AiNAJdr0KMmHymC9eqBKD1t1i86wIDAQAB
AoGAIu1ekNvEsqNkyFSpZHE5n0DEjyR7IXKFvEoziiD5nO7Q0n8MRXM2B/usB06R
D8/2uiwTRt6ktMp5mMc/dQZhEwlKxwbFEkg+XUKU+lAEghVWpiOwTTaALveIjgKE
eCY4WDJJjL1lU1WTUqOb8LDHofm2wSfQbyU5RD3/BszEXgECQQDd/kLJ9A3qbhiV
1IKGDCYcqpe45RI3Y0+256t+WBzSrv3HqO6vc3PLgl+SumpFoUe7S1QchGaz/nrm
EsZGUukZAkEA3ScSe6oFJmuzx3Z5yd6wWosbqPrzkRdjP/ZmzgqEWCfKX/OY2kiJ
eu9dNzyeQ6YApeFeQ1BKi9QtP23TOIEiowJAFgVT0L6x5rBXJf23mN55pVxSwpeO
kAn87VLb0yOgcFHFgNnEG4ljUiuzmVV+lzuhZvXY+R81JOO4gzwXiQBOeQJAA7Oo
uosxBOCepMMV7MwedZWIg/6XXyFeFu7/74j7iCI6X/rK3zSBoJ4rGEaae5Vmw2AP
XN8WMFr/2uTyuSpoMwJAHZiK8sLhEExsABriFpvfXG8ktlJ+ix4Y6dy3EojYtgFW
1ozWTYzUUIq5IEcPC7oqR+5FYSC42L8WRrkXaPpHcw==
-----END RSA PRIVATE KEY-----")

let privateKeysConsumer2 =
    Map.empty.
        Add("Rsa1024key2", @"-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQC59k14/QlPcOgvvl7rujdm2RFiMgc1cwbZ5yUGYBrGFd6eVnvH
16Q7igyde5e0WytEYpeabB1KcRVgDSkElNkgs9Ns7UcYaKbnRqHRgPjMdyu+X70m
HOeja2iMjPxR+hrnZFrNM2x0MevHK26WORUPFHIE/2GVt86a3z0d8xj0qQIDAQAB
AoGBALiRo3cP/eug7nJkiiWA33fuvfguG0WLcyNW7UKUpD4yeo/A2n4Qo2qMq9Sq
VHmnexwWls2nvLKj5kk9BpcLfSvsO4eRHCQHnCwYdEnUeXYzeFD/9vlFXrYuCRO0
6cOqfSZZmxvF92Si5JGBmb5xnQr66Nvf7M4UCdCHzyQPDuBZAkEA5xGaYJt8/YHj
JVgxdznkiyCxWLgvGwgNq3qLR6XPlRQKYk4XSLzZNu23rkCcq+4UinSu1pDVIkS5
V1/U9gNrqwJBAM4GzMb7gzkU/cBz5KhTJPv9DcQnAOyCtm+/RhUOoDnfcrndFWBT
nEs4asnDT1gBxLkHiOUkivYOuz72EpC3LPsCQQCsaRILa3lDnprhzoB6OZQxy18I
l8VuIgAxJuqttybAUYe9+g6dk2tv9MfNGSDNmINzG8UpDEA7pZO1gifguISpAkAe
J3ChTv6NxDy/hjbZTBIFr6vsIalI9HivMleXjWR2E/Y+rdULHDGr8L3wed2LC/c2
/ZtTrl2IVe+h73IYLDcxAkBraoN02VKU0IN+2RhhyNJoVA+1Et8k/zkTC8D9R74P
lrZ/H1WdTMjPKaCkIta8NEBpxuoUSFm5zVyixf+T1kU7
-----END RSA PRIVATE KEY-----")


type ProducerKeyReader() =
    interface ICryptoKeyReader with

        member this.GetPublicKey(keyName) =
            { Key = Encoding.UTF8.GetBytes(publicKeys.Item keyName); Metadata = null }

        member this.GetPrivateKey(_, _) = raise (NotImplementedException())

type Consumer1KeyReader() =
    interface ICryptoKeyReader with
        member this.GetPublicKey(_) = raise (NotImplementedException())

        member this.GetPrivateKey(keyName, _) =
            { Key = Encoding.UTF8.GetBytes(privateKeysConsumer1.Item keyName); Metadata = null }

type Consumer2KeyReader() =
    interface ICryptoKeyReader with
        member this.GetPublicKey(_) = raise (NotImplementedException())

        member this.GetPrivateKey(keyName, _) =
            { Key = Encoding.UTF8.GetBytes(privateKeysConsumer2.Item keyName); Metadata = null }

[<ProtoContract>]
type BatchItemMetadata() =
    [<ProtoMember(3)>]
    member val PayloadSize = 0 with get, set

/// Decrypts a batch and cuts it right after its first item, so the second item cannot be deserialized
type FirstItemOnlyBatchDecryptor(inner: IMessageDecryptor) =
    interface IMessageDecryptor with
        member this.Decrypt(encryptedPayload) =
            let batch = inner.Decrypt encryptedPayload
            use stream = new MemoryStream(batch)
            let firstItem = Serializer.DeserializeWithLengthPrefix<BatchItemMetadata>(stream, PrefixStyle.Fixed32BigEndian)
            // keep a partial length prefix of the second item
            Array.sub batch 0 (int stream.Position + firstItem.PayloadSize + 2)


let consumeMessagesAndCheckEncryption (consumer: IConsumer<byte[]>) number consumerName =
    task {
        for i in 1..number do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Data)
            Log.Debug("{0} received {1}", consumerName, received)
            
            // Check EncryptionContext
            match message.EncryptionContext with
            | Some context ->
                Log.Debug("{0} message {1} EncryptionContext.IsEncrypted = {2}", consumerName, i, context.IsEncrypted)
                if context.IsEncrypted then
                    failwith $"Message {i} should not be encrypted, but IsEncrypted = true"
            | None ->
                failwith $"Message {i} should have EncryptionContext but it is None"
            
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} acknowledged {1}", consumerName, received)
            let expected = "Message #" + string i
            if received.StartsWith(expected) |> not then
                failwith $"Incorrect message expected {expected} received {received} consumer {consumerName}"
        
        Log.Debug("{0} consumed {1} messages, all EncryptionContext.IsEncrypted = false", consumerName, number)
    }


let getBrokerConsumerStats (topicName: string) (subscription: string) =
    task {
        let! (response : string) = commonHttpClient.GetStringAsync($"{pulsarHttpAddress}/admin/v2/persistent/{topicName}/stats")
        let consumer =
            JsonDocument.Parse(response).RootElement
                .GetProperty("subscriptions").GetProperty(subscription)
                .GetProperty("consumers").[0]
        return struct (consumer.GetProperty("msgOutCounter").GetInt64(),
                       consumer.GetProperty("unackedMessages").GetInt64(),
                       consumer.GetProperty("availablePermits").GetInt32())
    }

/// Polls the broker until the consumer stats (msgOutCounter, unackedMessages, availablePermits) match, returns the last observed stats
let waitForBrokerConsumerStats topicName subscription (expected: struct (int64 * int64 * int)) =
    task {
        let mutable stats = struct (0L, 0L, 0)
        let mutable attempts = 0
        while stats <> expected && attempts < 50 do
            do! Task.Delay 100
            let! (current : struct (int64 * int64 * int)) = getBrokerConsumerStats topicName subscription
            stats <- current
            attempts <- attempts + 1
        return stats
    }

[<Tests>]
let tests =
    testList "MessageCrypto" [
        testTask "Discarded undecryptable messages return flow permits" {
            Log.Debug("Started Discarded undecryptable messages return flow permits")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("discardConsumer")
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(2)
                    .CryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
                    .SubscribeAsync()

            let! (encryptedProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .MessageEncryptor(MessageEncryptor([|"Rsa1024key1"|], ProducerKeyReader()))
                    .CreateAsync()

            let! (plainProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync()

            // as many undecryptable messages as the consumer has permits, then one it can read
            let! (_ : MessageId) = encryptedProducer.SendAsync(Encoding.UTF8.GetBytes "encrypted 1")
            let! (_ : MessageId) = encryptedProducer.SendAsync(Encoding.UTF8.GetBytes "encrypted 2")
            let! (_ : MessageId) = plainProducer.SendAsync(Encoding.UTF8.GetBytes "plain")

            let cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10.0))
            let! (message : Message<byte[]>) =
                task {
                    try
                        return! consumer.ReceiveAsync(cts.Token)
                    with :? OperationCanceledException ->
                        return failwith "Plain message was not delivered after the undecryptable ones were discarded"
                }
            cts.Dispose()
            Expect.equal (Encoding.UTF8.GetString message.Data) "plain" "payload"
            do! consumer.AcknowledgeAsync(message.MessageId)

            do! consumer.UnsubscribeAsync()
            do! encryptedProducer.DisposeAsync()
            do! plainProducer.DisposeAsync()
            Log.Debug("Ended Discarded undecryptable messages return flow permits")
        }

        testTask "Discarded undecryptable batch returns a permit for every message in it" {
            Log.Debug("Started Discarded undecryptable batch returns a permit for every message in it")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "test-subscription"
            let receiverQueueSize = 4
            let batchSize = 3

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("discardBatchConsumer")
                    .SubscriptionName(subscriptionName)
                    .ReceiverQueueSize(receiverQueueSize)
                    .CryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
                    .SubscribeAsync()

            let! (encryptedProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(10.0))
                    .MessageEncryptor(MessageEncryptor([|"Rsa1024key1"|], ProducerKeyReader()))
                    .CreateAsync()

            let! (messageIds : MessageId[]) =
                [| for i in 1..batchSize -> encryptedProducer.SendAsync(Encoding.UTF8.GetBytes $"encrypted {i}") |]
                |> Task.WhenAll
            let entries = messageIds |> Array.distinctBy (fun id -> id.LedgerId, id.EntryId)
            Expect.hasLength entries 1 "all messages should be published in a single batch"

            // the broker charges one permit per message in the batch; once the batch has been
            // dispatched and discarded (acked) every permit has to be back at the broker
            let expected = struct (int64 batchSize, 0L, receiverQueueSize)
            let! (stats : struct (int64 * int64 * int)) = waitForBrokerConsumerStats topicName subscriptionName expected
            Expect.equal stats expected "broker consumer stats (msgOut, unacked, availablePermits) after the discard"

            do! consumer.UnsubscribeAsync()
            do! encryptedProducer.DisposeAsync()
            Log.Debug("Ended Discarded undecryptable batch returns a permit for every message in it")
        }

        testTask "Malformed batch returns only the permits of the messages it lost" {
            Log.Debug("Started Malformed batch returns only the permits of the messages it lost")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "test-subscription"
            let receiverQueueSize = 4
            let batchSize = 3

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("malformedBatchConsumer")
                    .SubscriptionName(subscriptionName)
                    .ReceiverQueueSize(receiverQueueSize)
                    .MessageDecryptor(FirstItemOnlyBatchDecryptor(MessageDecryptor(Consumer1KeyReader())))
                    .SubscribeAsync()

            let! (encryptedProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(10.0))
                    .MessageEncryptor(MessageEncryptor([|"Rsa1024key1"|], ProducerKeyReader()))
                    .CreateAsync()

            let! (messageIds : MessageId[]) =
                [| for i in 1..batchSize -> encryptedProducer.SendAsync(Encoding.UTF8.GetBytes $"encrypted {i}") |]
                |> Task.WhenAll
            let entries = messageIds |> Array.distinctBy (fun id -> id.LedgerId, id.EntryId)
            Expect.hasLength entries 1 "all messages should be published in a single batch"

            // the first item is queued for the application before the batch breaks, so only the
            // permits of the two lost items come back with the discard
            let afterDiscard = struct (int64 batchSize, 0L, receiverQueueSize - 1)
            let! (stats : struct (int64 * int64 * int)) = waitForBrokerConsumerStats topicName subscriptionName afterDiscard
            Expect.equal stats afterDiscard "broker consumer stats (msgOut, unacked, availablePermits) after the discard"

            let cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10.0))
            let! (message : Message<byte[]>) = consumer.ReceiveAsync(cts.Token)
            cts.Dispose()
            Expect.equal (Encoding.UTF8.GetString message.Data) "encrypted 1" "the item queued before the batch broke"

            // consuming the queued item returns its own permit, nothing more
            let afterConsume = struct (int64 batchSize, 0L, receiverQueueSize)
            let! (stats : struct (int64 * int64 * int)) = waitForBrokerConsumerStats topicName subscriptionName afterConsume
            Expect.equal stats afterConsume "broker consumer stats (msgOut, unacked, availablePermits) after consuming the queued item"

            do! consumer.UnsubscribeAsync()
            do! encryptedProducer.DisposeAsync()
            Log.Debug("Ended Malformed batch returns only the permits of the messages it lost")
        }

        testTask "Pending receive gets the item that survived a malformed batch" {
            Log.Debug("Started Pending receive gets the item that survived a malformed batch")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "test-subscription"
            let receiverQueueSize = 4
            let batchSize = 3

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName("pendingReceiveMalformedBatchConsumer")
                    .SubscriptionName(subscriptionName)
                    .ReceiverQueueSize(receiverQueueSize)
                    .MessageDecryptor(FirstItemOnlyBatchDecryptor(MessageDecryptor(Consumer1KeyReader())))
                    .SubscribeAsync()

            let! (encryptedProducer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(batchSize)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(10.0))
                    .MessageEncryptor(MessageEncryptor([|"Rsa1024key1"|], ProducerKeyReader()))
                    .CreateAsync()

            // the receive is already waiting when the malformed batch arrives
            let cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10.0))
            let receiveTask = consumer.ReceiveAsync(cts.Token)
            do! Task.Delay 200

            let! (messageIds : MessageId[]) =
                [| for i in 1..batchSize -> encryptedProducer.SendAsync(Encoding.UTF8.GetBytes $"encrypted {i}") |]
                |> Task.WhenAll
            let entries = messageIds |> Array.distinctBy (fun id -> id.LedgerId, id.EntryId)
            Expect.hasLength entries 1 "all messages should be published in a single batch"

            let! (message : Message<byte[]>) =
                task {
                    try
                        return! receiveTask
                    with :? OperationCanceledException ->
                        return failwith "The pending receive never got the item that survived the malformed batch"
                }
            cts.Dispose()
            Expect.equal (Encoding.UTF8.GetString message.Data) "encrypted 1" "the item queued before the batch broke"

            // the delivered item returned its permit, the two lost ones came back with the discard
            let expected = struct (int64 batchSize, 0L, receiverQueueSize)
            let! (stats : struct (int64 * int64 * int)) = waitForBrokerConsumerStats topicName subscriptionName expected
            Expect.equal stats expected "broker consumer stats (msgOut, unacked, availablePermits) after the delivery"

            do! consumer.UnsubscribeAsync()
            do! encryptedProducer.DisposeAsync()
            Log.Debug("Ended Pending receive gets the item that survived a malformed batch")
        }

        testTask "Simple encryption send message" {
            Log.Debug("Started Simple encryption send message")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "MessageCrypto"

            let! producer =
              client.NewProducer()
                  .Topic(topicName)
                  .MessageEncryptor(MessageEncryptor([|"Rsa1024key1"|], ProducerKeyReader()))
                  .CreateAsync()


            let! consumer =
              client.NewConsumer()
                  .Topic(topicName)
                  .MessageDecryptor(MessageDecryptor(Consumer1KeyReader()))
                  .ConsumerName(consumerName).SubscriptionName("test-subscription")
                  .SubscribeAsync()


            let producerTask =
              Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages consumerName
                    } :> Task)

            let consumerTask =
              Task.Run(fun () ->
                  task {
                      do! consumeMessagesAndCheckEncryption consumer numberOfMessages consumerName
                  } :> Task)

            do! Task.WhenAll(producerTask, consumerTask)
            do! Task.Delay 100
            Log.Debug("Ended Simple encryption send message")
        }

        testTask "Encryption send message with two public key and receive two different consumer" {
            Log.Debug("Started Encryption send message with two public key and receive two different consumer")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "MessageCrypto"

            let encryptor = MessageEncryptor([|"Rsa1024key1"; "Rsa1024key2"|], ProducerKeyReader())
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .MessageEncryptor(encryptor)
                    .CreateAsync()


            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .MessageDecryptor(MessageDecryptor(Consumer1KeyReader()))
                    .ConsumerName(consumerName).SubscriptionName("test-subscription")
                    .SubscribeAsync()


            let producerTask =
              Task.Run(fun () ->
                  task {
                      do! produceMessages producer numberOfMessages consumerName
                  } :> Task)

            let consumer1Task =
              Task.Run(fun () ->
                  task {
                      do! consumeMessagesAndCheckEncryption consumer1 numberOfMessages consumerName
                  } :> Task)

            do! Task.WhenAll(producerTask, consumer1Task)
            do! Task.Delay 100

            do! consumer1.DisposeAsync().AsTask()

            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .MessageDecryptor(MessageDecryptor(Consumer2KeyReader()))
                    .ConsumerName(consumerName).SubscriptionName("test-subscription")
                    .SubscribeAsync()


            post (producer :?> ProducerImpl<byte[]>).Mb (Tick (UpdateEncryptionKeys encryptor))

            let producerTask2 =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages consumerName
                    } :> Task)

            let consumer2Task =
              Task.Run(fun () ->
                  task {
                      do! consumeMessagesAndCheckEncryption consumer2 numberOfMessages consumerName
                  } :> Task)

            do! Task.WhenAll(producerTask2, consumer2Task)
            do! Task.Delay 100
            Log.Debug("Ended Encryption send message with two public key and receive two different consumer")
        }

        testTask "Encryption send message and consume on fail" {
            Log.Debug("Started Encryption send message and consume on fail")
            let client = getClient ()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10
            let consumerName = "MessageCrypto"
            let compressionType = CompressionType.LZ4

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(400.0))
                    .BatchingMaxMessages(numberOfMessages)
                    .EnableBatching(true)
                    .MessageEncryptor(MessageEncryptor([|"Rsa1024key3"|], ProducerKeyReader()))
                    .CompressionType(compressionType)
                    .CreateAsync()


            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .MessageDecryptor(MessageDecryptor(Consumer1KeyReader()))
                    .CryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                    .ConsumerName(consumerName).SubscriptionName("test-subscription")
                    .SubscribeAsync()


            do! fastProduceMessages producer numberOfMessages consumerName

            let! (message : Message<byte[]>) = consumer.ReceiveAsync()
            Expect.isTrue message.EncryptionContext.IsSome "Message must contain EncryptionContext"
            let context = message.EncryptionContext.Value
            let batchSize = context.BatchSize |> int
            Expect.equal batchSize numberOfMessages "Message must contain producer batch message"
            Expect.isNonEmpty context.Param ""
            Expect.isNonEmpty context.Keys ""
            Expect.equal context.CompressionType compressionType ""
            Expect.equal context.IsEncrypted true "Message should remain encrypted when decryption fails"
            do! Task.Delay 100
            Log.Debug("Ended Encryption send message and consume on fail")
        }
    ]
