module Pulsar.Client.UnitTests.Internal.BatchMessageContainerTests

open Expecto
open Expecto.Flip
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

let private add (container: KeyBasedBatchMessageContainer<int64>) key sequenceId =
    let messageKey = { PartitionKey = %key; IsBase64Encoded = false }
    container.Add {
        Message = MessageBuilder(sequenceId, [||], Some messageKey)
        SequenceId = %sequenceId
        Tcs = None
    } |> ignore

let private summarize (batches: seq<OpSendMsgWrapper<int64>>) =
    batches
    |> Seq.map (fun batch ->
        use stream = batch.Stream
        let values = batch.OpSendMsg |> Array.map (fun struct(_, message, _) -> message.Value)
        batch.SequenceId, batch.HighestSequenceId, values)
    |> Seq.toArray

[<Tests>]
let tests =
    testList "BatchMessageContainerTests" [
        test "Empty key container produces no batches" {
            let container = KeyBasedBatchMessageContainer<int64>("test", ProducerConfiguration.Default)
            container.CreateOpSendMsgs() |> summarize |> Expect.isEmpty ""
        }

        test "Key batches are sorted by maxima without reordering messages" {
            let container = KeyBasedBatchMessageContainer<int64>("test", ProducerConfiguration.Default)
            add container "a" 9L
            add container "b" 4L
            add container "c" 7L
            add container "a" 1L
            add container "b" 2L
            container.CreateOpSendMsgs()
            |> summarize
            |> Expect.equal "" [|
                %4L, %4L, [| 4L; 2L |]
                %7L, %7L, [| 7L |]
                %9L, %9L, [| 9L; 1L |]
            |]
        }

        test "Equal maxima retain all key batches" {
            let container = KeyBasedBatchMessageContainer<int64>("test", ProducerConfiguration.Default)
            for index in 0 .. 31 do
                add container (string index) (int64 index)
                add container (string index) 100L
            container.CreateOpSendMsgs()
            |> summarize
            |> Array.map (fun (_, _, values) -> values[0])
            |> Array.sort
            |> Expect.equal "" [| 0L .. 31L |]
        }

        test "Key batches are collected before enumeration" {
            let container = KeyBasedBatchMessageContainer<int64>("test", ProducerConfiguration.Default)
            add container "a" 2L
            add container "b" 1L
            let batches = container.CreateOpSendMsgs()
            add container "c" 0L
            batches
            |> summarize
            |> Expect.equal "" [| %1L, %1L, [| 1L |]; %2L, %2L, [| 2L |] |]
        }

        test "Each key batch is serialized only when requested" {
            let container = KeyBasedBatchMessageContainer<int64>("test", ProducerConfiguration.Default)
            let payloads = [| [| 0uy |]; [| 0uy |] |]
            for index in 0 .. 1 do
                let key = { PartitionKey = %(string index); IsBase64Encoded = false }
                container.Add {
                    Message = MessageBuilder(int64 index, payloads[index], Some key)
                    SequenceId = %(int64 index)
                    Tcs = None
                } |> ignore
            let batches = container.CreateOpSendMsgs()
            use enumerator = batches.GetEnumerator()
            payloads[0][0] <- 1uy
            enumerator.MoveNext() |> Expect.isTrue ""
            use firstStream = enumerator.Current.Stream
            firstStream.ToArray() |> Array.last |> Expect.equal "" 1uy
            payloads[1][0] <- 2uy
            enumerator.MoveNext() |> Expect.isTrue ""
            use secondStream = enumerator.Current.Stream
            secondStream.ToArray() |> Array.last |> Expect.equal "" 2uy
            enumerator.MoveNext() |> Expect.isFalse ""
        }
    ]
