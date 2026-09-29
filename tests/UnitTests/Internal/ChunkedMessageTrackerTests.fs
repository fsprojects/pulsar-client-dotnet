module Pulsar.Client.UnitTests.Internal.ChunkedMessageTrackerTests

open System
open System.IO
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open FSharp.UMX

[<Tests>]
let tests =

    let testMetadata =
        {
            NumMessages = 0
            NumChunks = 0
            TotalChunkMsgSize = 0
            HasNumMessagesInBatch = false
            CompressionType = CompressionType.None
            UncompressedMessageSize = 0
            SchemaVersion = None
            SequenceId = %0L
            ChunkId = %0
            PublishTime = %DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            Uuid = %""
            EncryptionKeys = [||]
            EncryptionParam = [||]
            EncryptionAlgo = ""
            EventTime = Nullable()
            OrderingKey = [||]
            ReplicatedFrom = ""
            ProducerName = ""
            NullValue = false
        }

    let testRawMessage =
        {
            MessageId = Unchecked.defaultof<MessageId>
            ConsumerEpoch = Nullable()
            Metadata = Unchecked.defaultof<Metadata>
            RedeliveryCount = 0
            Payload = new MemoryStream [||]
            MessageKey = ""
            IsKeyBase64Encoded = false
            CheckSumValid = false
            Properties = null
            AckSet = null
        }
    let testCodec = CompressionCodec.get CompressionType.None

    testList "ChunkedMessageTracker" [
        test "One-message chunk works" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_1", 2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata = { testMetadata with NumChunks = 1; TotalChunkMsgSize = 1 }
            let msgId = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage = { testRawMessage with MessageId = msgId; Metadata = metadata; Payload = new MemoryStream [| 1uy |] }
            let context = tracker.GetContext(metadata)
            match context with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessage, msgId, ctx, testCodec) with
                | Some (bytes, newMsgId) ->
                    Expect.sequenceEqual "" [| 1uy |] bytes
                    Expect.sequenceEqual "" [| msgId |] newMsgId.ChunkMessageIds.Value
                | None ->
                    failwith "No Message received"
            | _ ->
                failwith "No context"
        }

        test "Two-message chunk works" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_2", 2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2 }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            let context = tracker.GetContext(metadata1)
            match context with
            | Ok ctx ->
                let firstTry = tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec)
                Expect.isNone "" firstTry
            | _ ->
                failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; ChunkId = %1 }
            let msgId2 = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage2 = { testRawMessage with MessageId = msgId2; Metadata = metadata2; Payload = new MemoryStream [| 2uy |] }
            let context = tracker.GetContext(metadata2)
            match context with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessage2, msgId2, ctx, testCodec) with
                | Some (bytes, newMsgId) ->
                    Expect.sequenceEqual "" [| 1uy; 2uy |] bytes
                    Expect.sequenceEqual "" [| msgId1; msgId2 |] newMsgId.ChunkMessageIds.Value
                | None ->
                    failwith "No Message received"
            | _ ->
                failwith "No context"
        }

        test "Tracker overflow works as expected" {
            let mutable xShouldAck = true
            let mutable xMsgId = Unchecked.defaultof<MessageId>
            let ackOrTrack msgId shouldAck =
                xShouldAck <- shouldAck
                xMsgId <- msgId
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_3", 1, false, TimeSpan.Zero, ackOrTrack) // one pending context allowed
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1" }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            let context = tracker.GetContext(metadata1)
            match context with
            | Ok ctx ->
                let firstTry = tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec)
                Expect.isNone "" firstTry
            | _ ->
                failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"2" }
            let msgId2 = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage2 = { testRawMessage with MessageId = msgId2; Metadata = metadata2; Payload = new MemoryStream [| 2uy |] }
            let context = tracker.GetContext(metadata2)
            match context with
            | Ok ctx ->
                let secondTry = tracker.MessageReceived(rawMessage2, msgId2, ctx, testCodec)
                Expect.isNone "" secondTry
            | _ ->
                failwith "No context"
            let metadata3 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1"; ChunkId = %1 }
            let context = tracker.GetContext(metadata3)
            Expect.isError "" context
            Expect.isFalse "" xShouldAck
            Expect.equal "" xMsgId msgId1
        }

        testTask "Tracker timeout works as expected" {
            let mutable xShouldAck = false
            let mutable xMsgId = Unchecked.defaultof<MessageId>
            let ackOrTrack msgId shouldAck =
                xShouldAck <- shouldAck
                xMsgId <- msgId
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_4", 2, false, TimeSpan.FromMilliseconds(50.0), ackOrTrack) // one pending context allowed
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1" }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            let context = tracker.GetContext(metadata1)
            match context with
            | Ok ctx ->
                let firstTry = tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec)
                Expect.isNone "" firstTry
            | _ ->
                failwith "No context"
            do! Task.Delay 70
            tracker.RemoveExpireIncompleteChunkedMessages()
            let metadata3 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1"; ChunkId = %1 }
            let context = tracker.GetContext(metadata3)
            Expect.isError "" context
            Expect.isTrue "" xShouldAck
            Expect.equal "" xMsgId msgId1
        }

        testTask "Out-of-order chunk does not leave a dangling pending entry" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_6", 2, false, TimeSpan.FromMilliseconds(10.0), fun _ _ -> ())
            let metadata1 = { testMetadata with NumChunks = 3; TotalChunkMsgSize = 3; Uuid = %"1" }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadata1) with
            | Ok ctx -> tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 3; TotalChunkMsgSize = 3; Uuid = %"1"; ChunkId = %2 }
            tracker.GetContext(metadata2) |> Expect.isError ""
            do! Task.Delay 30
            tracker.RemoveExpireIncompleteChunkedMessages()
            // the uuid can be reused afterwards
            tracker.GetContext(metadata1) |> Expect.isOk ""
        }

        testTask "Redelivered first chunk restarts the message without duplicating the pending entry" {
            let mutable acked = ResizeArray<MessageId>()
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_7", 1, false, TimeSpan.FromMilliseconds(10.0), fun msgId _ -> acked.Add msgId)
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1" }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadata1) with
            | Ok ctx -> tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // chunk 0 delivered again, e.g. after a reconnect
            match tracker.GetContext(metadata1) with
            | Ok ctx -> tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            Expect.isEmpty "" acked
            let metadata2 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1"; ChunkId = %1 }
            let msgId2 = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage2 = { testRawMessage with MessageId = msgId2; Metadata = metadata2; Payload = new MemoryStream [| 2uy |] }
            match tracker.GetContext(metadata2) with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessage2, msgId2, ctx, testCodec) with
                | Some (bytes, _) -> Expect.sequenceEqual "" [| 1uy; 2uy |] bytes
                | None -> failwith "No Message received"
            | _ -> failwith "No context"
            do! Task.Delay 30
            tracker.RemoveExpireIncompleteChunkedMessages()
            // a new message must not evict anything: the queue is empty
            let metadata3 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"2" }
            tracker.GetContext(metadata3) |> Expect.isOk ""
            Expect.isEmpty "" acked
        }

        testTask "Restarted message does not hide an older expired message behind it" {
            let acked = ResizeArray<MessageId>()
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_10", 0, false, TimeSpan.FromMilliseconds(100.0), fun msgId _ -> acked.Add msgId)
            let metadataA = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"A" }
            let msgIdA = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessageA = { testRawMessage with MessageId = msgIdA; Metadata = metadataA; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadataA) with
            | Ok ctx -> tracker.MessageReceived(rawMessageA, msgIdA, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadataB = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"B" }
            let msgIdB = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessageB = { testRawMessage with MessageId = msgIdB; Metadata = metadataB; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadataB) with
            | Ok ctx -> tracker.MessageReceived(rawMessageB, msgIdB, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // both messages are expired now
            do! Task.Delay 150
            // chunk 0 of A delivered again, A is restarted and no longer expired
            match tracker.GetContext(metadataA) with
            | Ok ctx -> tracker.MessageReceived(rawMessageA, msgIdA, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // B is expired, A is not; without any wait in between the outcome does not depend on scheduling
            tracker.RemoveExpireIncompleteChunkedMessages()
            Expect.sequenceEqual "" [ msgIdB ] acked
            let metadataA1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"A"; ChunkId = %1 }
            tracker.GetContext(metadataA1) |> Expect.isOk ""
        }

        test "Overflow evicts the oldest message, not a restarted one" {
            let acked = ResizeArray<MessageId>()
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_11", 2, false, TimeSpan.Zero, fun msgId _ -> acked.Add msgId)
            let metadataA = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"A" }
            let msgIdA = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessageA = { testRawMessage with MessageId = msgIdA; Metadata = metadataA; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadataA) with
            | Ok ctx -> tracker.MessageReceived(rawMessageA, msgIdA, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadataB = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"B" }
            let msgIdB = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessageB = { testRawMessage with MessageId = msgIdB; Metadata = metadataB; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadataB) with
            | Ok ctx -> tracker.MessageReceived(rawMessageB, msgIdB, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // chunk 0 of A delivered again, A is now younger than B
            match tracker.GetContext(metadataA) with
            | Ok ctx -> tracker.MessageReceived(rawMessageA, msgIdA, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // a third message overflows the queue, B must go
            let metadataC = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"C" }
            tracker.GetContext(metadataC) |> Expect.isOk ""
            Expect.sequenceEqual "" [ msgIdB ] acked
            let metadataA1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"A"; ChunkId = %1 }
            let msgIdA1 = { EntryId = %3L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessageA1 = { testRawMessage with MessageId = msgIdA1; Metadata = metadataA1; Payload = new MemoryStream [| 2uy |] }
            match tracker.GetContext(metadataA1) with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessageA1, msgIdA1, ctx, testCodec) with
                | Some (bytes, _) -> Expect.sequenceEqual "" [| 1uy; 2uy |] bytes
                | None -> failwith "No Message received"
            | _ -> failwith "No context"
        }

        test "Chunk id equal to the chunk count is rejected" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_8", 2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1" }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadata1) with
            | Ok ctx -> tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1"; ChunkId = %2 }
            tracker.GetContext(metadata2) |> Expect.isError ""
        }

        test "Chunk with a changed chunk count is rejected" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_9", 2, true, TimeSpan.Zero, fun _ _ -> ())
            // chunk 0 sizes the context for two chunks
            let metadata0 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 3; Uuid = %"1" }
            let msgId0 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage0 = { testRawMessage with MessageId = msgId0; Metadata = metadata0; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadata0) with
            | Ok ctx -> tracker.MessageReceived(rawMessage0, msgId0, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // an in-order chunk claiming a bigger count would keep the message open
            // and let the next chunk index past the chunk id array
            let metadata1 = { testMetadata with NumChunks = 4; TotalChunkMsgSize = 3; Uuid = %"1"; ChunkId = %1 }
            tracker.GetContext(metadata1) |> Expect.isError ""
            // the message is dropped and the uuid can start over
            tracker.GetContext(metadata0) |> Expect.isOk ""
        }

        test "Wrong chunk order handled as expected" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_5", 2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata1 = { testMetadata with NumChunks = 3; TotalChunkMsgSize = 3 }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 1uy |] }
            let context = tracker.GetContext(metadata1)
            match context with
            | Ok ctx ->
                let firstTry = tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec)
                Expect.isNone "" firstTry
            | _ ->
                failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 3; TotalChunkMsgSize = 3; ChunkId = %2 }
            let context = tracker.GetContext(metadata2)
            Expect.isError "" context
        }

        test "Chunk with a changed total size is rejected" {
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_12", 2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata0 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; Uuid = %"1" }
            let msgId0 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage0 = { testRawMessage with MessageId = msgId0; Metadata = metadata0; Payload = new MemoryStream [| 1uy |] }
            match tracker.GetContext(metadata0) with
            | Ok ctx -> tracker.MessageReceived(rawMessage0, msgId0, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 3; Uuid = %"1"; ChunkId = %1 }
            tracker.GetContext(metadata1) |> Expect.isError ""
            // the message is dropped and the uuid can start over
            tracker.GetContext(metadata0) |> Expect.isOk ""
        }

        test "Chunk exceeding the declared total size is discarded" {
            let tracked = ResizeArray<MessageId * bool>()
            let tracker = ChunkedMessageTracker("ChunkedMessageTracker_13", 2, true, TimeSpan.Zero, fun msgId autoAck -> tracked.Add (msgId, autoAck))
            // 16 bytes is the smallest array the pool hands out, so the buffer has no slack
            let metadata0 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 16; Uuid = %"1" }
            let msgId0 = { EntryId = %1L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage0 = { testRawMessage with MessageId = msgId0; Metadata = metadata0; Payload = new MemoryStream (Array.create 16 1uy) }
            match tracker.GetContext(metadata0) with
            | Ok ctx -> tracker.MessageReceived(rawMessage0, msgId0, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 16; Uuid = %"1"; ChunkId = %1 }
            let msgId1 = { EntryId = %2L; LedgerId = %1L; Type = Single; Partition = 0; TopicName = %""; ChunkMessageIds = None  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = new MemoryStream [| 2uy |] }
            match tracker.GetContext(metadata1) with
            | Ok ctx -> tracker.MessageReceived(rawMessage1, msgId1, ctx, testCodec) |> Expect.isNone ""
            | _ -> failwith "No context"
            // the offending chunk is handed back for tracking, the message is dropped and the uuid can start over
            Expect.sequenceEqual "" [ msgId1, false ] tracked
            match tracker.GetContext(metadata1) with
            | Ok _ -> failwith "Context survived the overrun"
            | Error _ -> ()
            tracker.GetContext(metadata0) |> Expect.isOk ""
        }
    ]