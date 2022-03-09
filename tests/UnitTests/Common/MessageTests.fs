module Pulsar.Client.UnitTests.Common.MessageTests

open Expecto
open Expecto.Flip
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
    ]