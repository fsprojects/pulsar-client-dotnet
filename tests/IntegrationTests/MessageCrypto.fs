module Pulsar.Client.IntegrationTests.MessageCrypto

open System
open System.Text
open Expecto
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Crypto
open Pulsar.Client.IntegrationTests.Common

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
            KeyInfo(key = Encoding.UTF8.GetBytes(publicKeys.Item keyName), metadata = null)

        member this.GetPrivateKey(_, _) = raise (NotImplementedException())

type Consumer1KeyReader() =
    interface ICryptoKeyReader with
        member this.GetPublicKey(_) = raise (NotImplementedException())

        member this.GetPrivateKey(keyName, _) =
            KeyInfo(key = Encoding.UTF8.GetBytes(privateKeysConsumer1.Item keyName), metadata = null)

type Consumer2KeyReader() =
    interface ICryptoKeyReader with
        member this.GetPublicKey(_) = raise (NotImplementedException())

        member this.GetPrivateKey(keyName, _) =
            KeyInfo(key = Encoding.UTF8.GetBytes(privateKeysConsumer2.Item keyName), metadata = null)


[<Tests>]
let tests =
    testList "MessageCrypto" [
        testAsync "Simple encryption send message" {
              let client = getClient ()
              let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
              let numberOfMessages = 10
              let consumerName = "MessageCrypto"

              let! producer =
                  client.NewProducer()
                      .Topic(topicName)
                      .AddMessageEncrypt(MessageEncryptNSec([|"Rsa1024key1"|], ProducerKeyReader()))
                      .CreateAsync()
                  |> Async.AwaitTask

              let! consumer =
                  client.NewConsumer()
                      .Topic(topicName)
                      .AddMessageDecrypt(MessageDecryptNSec(Consumer1KeyReader()))
                      .ConsumerName(consumerName).SubscriptionName("test-subscription")
                      .SubscribeAsync()
                  |> Async.AwaitTask

              let producerTask =
                  Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages consumerName
                        } :> Task)

              let consumerTask =
                  Task.Run(fun () ->
                      task {
                          do! consumeMessages consumer numberOfMessages consumerName
                      } :> Task)

              do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
              do! Async.Sleep 100
          } 

        testAsync "Encryption send message with two public key and receive two different consumer" {
              let client = getClient ()
              let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
              let numberOfMessages = 10
              let consumerName = "MessageCrypto"

              let! producer =
                  client.NewProducer()
                      .Topic(topicName)
                      .AddMessageEncrypt(MessageEncryptNSec([|"Rsa1024key1"; "Rsa1024key2"|], ProducerKeyReader()))
                      .CreateAsync()
                  |> Async.AwaitTask

              let! consumer1 =
                  client.NewConsumer()
                      .Topic(topicName)
                      .AddMessageDecrypt(MessageDecryptNSec(Consumer1KeyReader()))
                      .ConsumerName(consumerName).SubscriptionName("test-subscription")
                      .SubscribeAsync()
                  |> Async.AwaitTask

              let producerTask =
                  Task.Run(fun () ->
                        task {
                            do! produceMessages producer numberOfMessages consumerName
                        } :> Task)

              let consumer1Task =
                  Task.Run(fun () ->
                      task {
                          do! consumeMessages consumer1 numberOfMessages consumerName
                      } :> Task)

              do! Task.WhenAll(producerTask, consumer1Task) |> Async.AwaitTask
              do! Async.Sleep 100
              
              do! consumer1.DisposeAsync().AsTask() |>  Async.AwaitTask
              
              let! consumer2 =
                  client.NewConsumer()
                      .Topic(topicName)
                      .AddMessageDecrypt(MessageDecryptNSec(Consumer2KeyReader()))
                      .ConsumerName(consumerName).SubscriptionName("test-subscription")
                      .SubscribeAsync()
                  |> Async.AwaitTask
              
              producer.UpdateEncryptionKeys()

              
              let producerTask2 =
                  Task.Run(fun () ->
                      task {
                          do! produceMessages producer numberOfMessages consumerName
                      } :> Task)

              let consumer2Task =
                  Task.Run(fun () ->
                      task {
                          do! consumeMessages consumer2 numberOfMessages consumerName
                      } :> Task)

              do! Task.WhenAll(producerTask2, consumer2Task) |> Async.AwaitTask
              do! Async.Sleep 100
          }
        
        testAsync "Encryption send message and consume on fail" {
              let client = getClient ()
              let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
              let numberOfMessages = 10
              let consumerName = "MessageCrypto"
              let compressionType = CompressionType.LZ4
              
              let! producer =
                  client.NewProducer()
                      .Topic(topicName)
                      .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100.0))
                      .BatchingMaxMessages(numberOfMessages)
                      .EnableBatching(true)
                      .AddMessageEncrypt(MessageEncryptNSec([|"Rsa1024key3"|], ProducerKeyReader()))
                      .CompressionType(compressionType)
                      .CreateAsync()
                  |> Async.AwaitTask

              let! consumer =
                  client.NewConsumer()
                      .Topic(topicName)
                      .AddMessageDecrypt(MessageDecryptNSec(Consumer1KeyReader()))
                      .CryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                      .ConsumerName(consumerName).SubscriptionName("test-subscription")
                      .SubscribeAsync()
                  |> Async.AwaitTask

              do! fastProduceMessages producer numberOfMessages consumerName |> Async.AwaitTask

              let consumerTask =
                  Task.Run(fun () ->
                      task {
                          let! message = consumer.ReceiveAsync()
                          Expect.isTrue message.EncryptionContext.IsSome "Message must contain EncryptionContext"
                          let context = message.EncryptionContext.Value
                          let batchSize = context.BatchSize |> int
                          Expect.equal batchSize numberOfMessages "Message must contain producer batch message"
                          Expect.isNonEmpty context.Param ""
                          Expect.isNonEmpty context.Keys ""
                          Expect.equal context.CompressionType compressionType ""
                      } :> Task)

              do! Task.WhenAll(consumerTask) |> Async.AwaitTask
              do! Async.Sleep 100
          } 
    ]
