module Pulsar.Client.UnitTests.Internal.ChunkedMessageTrackerTests

open System
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
            PublishTime = DateTime.UtcNow
            Uuid = %""
            EncryptionKeys = [||]
            EncryptionParam = [||]
            EncryptionAlgo = ""
        }
        
    let testRawMessage =
        {
            MessageId = Unchecked.defaultof<MessageId>
            Metadata = Unchecked.defaultof<Metadata>
            RedeliveryCount = 0u
            Payload = [||]
            MessageKey = ""
            IsKeyBase64Encoded = false
            CheckSumValid = false
            Properties = null
            AckSet = null
        }
    let testCodec = CompressionCodec.get CompressionType.None
    
    ftestList "ChunkedMessageTracker" [
        test "One-message chunk works" {
            let tracker = ChunkedMessageTracker(2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata = { testMetadata with NumChunks = 1; TotalChunkMsgSize = 1 }
            let msgId = { EntryId = %1L; LedgerId = %1L; Type = Individual; Partition = 0; TopicName = %""  }
            let rawMessage = { testRawMessage with MessageId = msgId; Metadata = metadata; Payload = [| 1uy |] }
            let context = tracker.GetContext(metadata)
            match context with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessage, ctx, testCodec) with
                | Some bytes ->
                    Expect.sequenceEqual "" [| 1uy |] bytes
                | None ->
                    failwith "No Message received"
            | _ ->
                failwith "No context"
        }
        
        test "Two-message chunk works" {
            let tracker = ChunkedMessageTracker(2, true, TimeSpan.Zero, fun _ _ -> ())
            let metadata1 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 1 }
            let msgId1 = { EntryId = %1L; LedgerId = %1L; Type = Individual; Partition = 0; TopicName = %""  }
            let rawMessage1 = { testRawMessage with MessageId = msgId1; Metadata = metadata1; Payload = [| 1uy |] }
            let context = tracker.GetContext(metadata1)
            match context with
            | Ok ctx ->
                let firstTry = tracker.MessageReceived(rawMessage1, ctx, testCodec)
                Expect.isNone "" firstTry
            | _ ->
                failwith "No context"
            let metadata2 = { testMetadata with NumChunks = 2; TotalChunkMsgSize = 2; ChunkId = %1 }
            let msgId2 = { EntryId = %2L; LedgerId = %1L; Type = Individual; Partition = 0; TopicName = %""  }
            let rawMessage2 = { testRawMessage with MessageId = msgId2; Metadata = metadata2; Payload = [| 2uy |] }
            let context = tracker.GetContext(metadata2)
            match context with
            | Ok ctx ->
                match tracker.MessageReceived(rawMessage2, ctx, testCodec) with
                | Some bytes ->
                    Expect.sequenceEqual "" [| 1uy; 2uy |] bytes
                | None ->
                    failwith "No Message received"
            | _ ->
                failwith "No context"
        }
        
    ]