module Pulsar.Client.UnitTests.Internal.ProducerImplTests

open System
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests

[<Tests>]
let tests =
    let rejectClose _ (struct (_, commandType): SendTask) =
        if commandType = BaseCommand.Type.CloseProducer then
            Task.FromException<PulsarResponseType>(ConnectException "Disconnected.")
        else
            Task.FromResult(
                PulsarResponseType.ProducerSuccess {
                    GeneratedProducerName = "test-producer"
                    SchemaVersion = None
                    LastSequenceId = %(-1L)
                })

    let expectDisposeRejected (producer: ProducerImpl<byte[]>) =
        task {
            try
                do! (producer :> IProducer<byte[]>).DisposeAsync()
                return failtest "Close completed; this test needs the broker close to fail"
            with Flatten ex ->
                match ex with
                | :? ConnectException -> return ()
                | other ->
                    return failtest $"Expected close to fail with {nameof ConnectException}, but got {other.GetType().Name}"
        }

    testList "ProducerImpl" [
        testTask "Batch timer stops posting after close fails" {
            let topic = TopicName("public/default/timer-leak")
            let producerConfig =
                { ProducerConfiguration.Default with
                    Topic = topic
                    BatchingEnabled = true
                    BatchingMaxPublishDelay = TimeSpan.FromMilliseconds(1.0) }
            let connection = { Defaults.clientCnx with SendAndWaitForReply = rejectClose }.Create()

            let! (producer: ProducerImpl<byte[]>) =
                Defaults.startBytesProducer producerConfig connection (Defaults.lookup.Create())

            do! expectDisposeRejected producer

            let takeAll () =
                let mutable count = 0
                let mutable pending = true
                while pending do
                    pending <- producer.Mb.Reader.TryRead() |> fst
                    if pending then
                        count <- count + 1
                count
            takeAll () |> ignore
            do! Task.Delay 10
            let arrived = takeAll ()
            if arrived > 1 then
                failtest $"Batch timer kept posting after close failed. {arrived} messages arrived after the mailbox stopped"
        }

        testTask "Producer is removed from the connection when close fails" {
            let topic = TopicName("public/default/close-deregister")
            let producerConfig = { ProducerConfiguration.Default with Topic = topic }
            let mutable removed = false
            let connection =
                { Defaults.clientCnx with
                    SendAndWaitForReply = rejectClose
                    RemoveProducer = fun _ -> removed <- true }.Create()

            let! (producer: ProducerImpl<byte[]>) =
                Defaults.startBytesProducer producerConfig connection (Defaults.lookup.Create())

            do! expectDisposeRejected producer

            if not removed then
                failtest "Producer stayed registered on the connection after close failed"
        }
    ]
