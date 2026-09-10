module Pulsar.Client.UnitTests.Common.MessageTests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Common
open FSharp.UMX
open Pulsar.Client.Internal

[<Tests>]
let tests =

    testList "MessageIdTests" [
        test "Less works correctly" {
            let msgId1 = { LedgerId = %1L; EntryId = %1L; Type = Single; Partition = 1; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with LedgerId = %2L; EntryId = %0L; }
            let msgId3 = { msgId1 with EntryId = %2L; Partition = 0; }
            let msgId4 = { msgId1 with LedgerId = %2L; Partition = 0; }
            let msgId5 = { msgId1 with Partition = 2; }
            let msgId6 = { msgId1 with Type = Batch(%(0), BatchMessageAcker.NullAcker) }
            Expect.isLessThan "" (msgId1, msgId2)
            Expect.isLessThan "" (msgId1, msgId3)
            Expect.isLessThan "" (msgId1, msgId4)
            Expect.isLessThan "" (msgId1, msgId5)
            Expect.isLessThan "" (msgId1, msgId6)
        }
        
        test "Less works correctly with batches" {
            let acker = BatchMessageAcker(0)
            let msgId1 = { LedgerId = %1L; EntryId = %1L; Type = Batch(%1, acker); Partition = 1; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with LedgerId = %2L; Type = Batch(%0, BatchMessageAcker.NullAcker) }
            let msgId3 = { msgId1 with EntryId = %2L; Type = Batch(%0, BatchMessageAcker(2)) }
            let msgId4 = { msgId1 with Type = Batch(%2, acker); Partition = 0; }
            Expect.isLessThan "" (msgId1, msgId2)
            Expect.isLessThan "" (msgId1, msgId3)
            Expect.isLessThan "" (msgId1, msgId4)
        }
        
        test "Equals works correctly" {
            let msgId1 = { LedgerId = %1L; EntryId = %1L; Type = Single; Partition = 1; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with TopicName = %"abcd" }
            let msgId3 = { msgId1 with ChunkMessageIds = Some [||] }
            let msgId4 = { msgId1 with Type = Batch(%(-1), BatchMessageAcker(2)) }
            Expect.equal "" msgId1 msgId2
            Expect.equal "" msgId1 msgId3
            Expect.equal "" msgId1 msgId4
        }
        
        test "Equals works correctly with batches" {
            let msgId1 = { LedgerId = %1L; EntryId = %1L; Type = Batch(%1, BatchMessageAcker(0)); Partition = 1; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { msgId1 with TopicName = %"abcd" }
            let msgId3 = { msgId1 with ChunkMessageIds = Some [||] }
            let msgId4 = { msgId1 with Type = Batch(%1, BatchMessageAcker(2)) }
            let msgId5 = { msgId1 with Type = Batch(%1, BatchMessageAcker.NullAcker) }
            Expect.equal "" msgId1 msgId2
            Expect.equal "" msgId1 msgId3
            Expect.equal "" msgId1 msgId4
            Expect.equal "" msgId1 msgId5
        }

        test "Serialization works correctly" {
            let msgId = { LedgerId = %1L; EntryId = %1L; Type = Single; Partition = 1;  TopicName = %""; 
                ChunkMessageIds = Some([| { LedgerId = %0L; EntryId = %0L; Type = Single; Partition = 0;  TopicName = %""; ChunkMessageIds = None} |]) }
            let msgIdData = msgId.ToByteArray()
            let deserialized = MessageId.FromByteArray msgIdData
            Expect.equal "" msgId deserialized
        }

        test "Batch MessageId serialization acker isolation" {
            let sharedAcker = BatchMessageAcker(2)
            let msgId1 = { LedgerId = %1L; EntryId = %1L; Type = Batch(%0, sharedAcker); Partition = 1; TopicName = %""; ChunkMessageIds = None }
            let msgId2 = { LedgerId = %1L; EntryId = %1L; Type = Batch(%1, sharedAcker); Partition = 1; TopicName = %""; ChunkMessageIds = None }
            
            let bytes1 = msgId1.ToByteArray()
            let bytes2 = msgId2.ToByteArray()
            
            let deserialized1 = MessageId.FromByteArray bytes1
            let deserialized2 = MessageId.FromByteArray bytes2
            
            match deserialized1.Type, deserialized2.Type with
            | Batch (_, acker1), Batch (_, acker2) ->
                // The deserialized ackers are different instances!
                Expect.isFalse "Ackers must not be the same instance" (obj.ReferenceEquals(acker1, acker2))
                
                // Let's ack individual indices
                let acked1 = acker1.AckIndividual(%0)
                let acked2 = acker2.AckIndividual(%1)
                
                // Neither one reports the whole batch is acknowledged
                Expect.isFalse "Acker 1 should not be fully acked" acked1
                Expect.isFalse "Acker 2 should not be fully acked" acked2
            | _ ->
                failwith "Deserialized message ID must be Batch type"
        }

        test "Message batching by count works correctly" {
            let messages = Messages(2, -1)
            let message = Message(MessageId.Earliest, [||], %"", false, EmptyProps, None, [||], %0L, [||], %0L, Nullable(), 0, "", "", Nullable(), Schema.BYTES(), fun () -> failwith "not implemented")
            messages.CanAdd(message) |> Expect.isTrue ""
            messages.Add(message)
            messages.CanAdd(message) |> Expect.isTrue ""
            messages.Add(message)
            messages.CanAdd(message) |> Expect.isFalse ""
        }

        test "Message batching by size works correctly" {
            let messages = Messages(-1, 2)
            let message = Message(MessageId.Earliest, [| 0uy |], %"", false, EmptyProps, None, [||], %0L, [||], %0L, Nullable(), 0, "", "", Nullable(), Schema.BYTES(), fun () -> failwith "not implemented")
            messages.CanAdd(message) |> Expect.isTrue ""
            messages.Add(message)
            messages.CanAdd(message) |> Expect.isTrue ""
            messages.Add(message)
            messages.CanAdd(message) |> Expect.isFalse ""
        }

        test "Reader schema is preserved by message copy helpers" {
            let readerSchema = Schema.BYTES()
            let message =
                Message(MessageId.Earliest, [| 0uy |], %"", false, EmptyProps, None, [| 1uy |], %0L,
                        [||], %0L, Nullable(), 0, "", "", Nullable(), readerSchema, fun () -> [| 0uy |])
            let copiedMessages = [
                message.WithMessageId(MessageId.Latest)
                message.WithData([| 1uy |])
                message.WithKey(%"key", false)
                message.WithProperties(readOnlyDict [ "key", "value" ])
            ]

            obj.ReferenceEquals(readerSchema, message.GetReaderSchema()) |> Expect.isTrue ""
            for copiedMessage in copiedMessages do
                obj.ReferenceEquals(readerSchema, copiedMessage.GetReaderSchema()) |> Expect.isTrue ""
        }
    ]