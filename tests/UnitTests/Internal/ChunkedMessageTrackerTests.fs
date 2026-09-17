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
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 1 }
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
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 1; Uuid = %"1" }
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
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 1; Uuid = %"1" }
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
    ]