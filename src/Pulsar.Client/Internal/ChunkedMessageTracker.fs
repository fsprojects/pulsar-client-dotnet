namespace Pulsar.Client.Internal

open System.Buffers
open System.Collections.Generic
open System.Diagnostics
open System.IO
open Pulsar.Client.Common
open FSharp.UMX
open Microsoft.Extensions.Logging

type internal ChunkedMessageCtx(totalChunksCount: int, totalChunksSize: int) =
    let chunkedMessageIds = Array.zeroCreate totalChunksCount
    let chunkedMsgBuffer: byte[] = ArrayPool.Shared.Rent totalChunksSize
    let receivedTime = Stopwatch.GetTimestamp()
    let mutable lastChunkId: ChunkId = %(-1)
    let mutable currentBufferLength = 0

    with
        member this.MessageReceived(msg: RawMessage) =
            // append the chunked payload and update lastChunkedMessage-id
            chunkedMessageIds[%msg.Metadata.ChunkId] <- msg.MessageId
            msg.Payload.Seek(0L, SeekOrigin.Begin) |> ignore
            msg.Payload.Read(chunkedMsgBuffer, currentBufferLength, int msg.Payload.Length) |> ignore
            currentBufferLength <- currentBufferLength + int msg.Payload.Length
            lastChunkId <- msg.Metadata.ChunkId
        member this.LastChunkId = lastChunkId
        member this.TotalChunksCount = totalChunksCount
        member this.TotalChunksSize = totalChunksSize
        member this.CurrentBufferLength = currentBufferLength
        member this.ChunkedMessageIds = chunkedMessageIds
        member this.Decompress (uncompressedSize, codec: ICompressionCodec) =
            codec.Decode(uncompressedSize, chunkedMsgBuffer, currentBufferLength)
        member this.ReceivedTime = receivedTime
        member this.Dispose() =
            ArrayPool.Shared.Return chunkedMsgBuffer

type internal ChunkedMessageTracker(prefix, maxPendingChunkedMessage, autoAckOldestChunkedMessageOnQueueFull, expireTimeOfIncompleteChunkedMessage,
                                    ackOrTrack) =
    let chunkedMessagesMap = Dictionary()
    let pendingChunkedMessageUuidQueue = LinkedList()
    let prefix = prefix + " ChunkedMessageTracker"

    let removeChunkMessage msgUuid (ctx: ChunkedMessageCtx) autoAck =
        // clean up pending chunked Message
        chunkedMessagesMap.Remove msgUuid |> ignore
        for msgId in ctx.ChunkedMessageIds do
            if box msgId |> isNull |> not then
                ackOrTrack msgId autoAck
        ctx.Dispose()

    let removeOldestPendingChunkedMessage() =
        let firstPendingMsgUuid = pendingChunkedMessageUuidQueue.First.Value
        Log.Logger.LogWarning("{0} RemoveOldestPendingChunkedMessage {1}", prefix, firstPendingMsgUuid)
        pendingChunkedMessageUuidQueue.RemoveFirst()
        match chunkedMessagesMap.TryGetValue firstPendingMsgUuid with
        | true, ctx -> removeChunkMessage firstPendingMsgUuid ctx autoAckOldestChunkedMessageOnQueueFull
        | _ -> ()

    member this.GetContext (metadata: Metadata) =
        if metadata.ChunkId = %0 then
            match chunkedMessagesMap.TryGetValue(metadata.Uuid) with
            | true, oldCtx ->
                // redelivered first chunk restarts the message, it becomes the youngest pending message
                oldCtx.Dispose()
                pendingChunkedMessageUuidQueue.Remove(metadata.Uuid) |> ignore
            | _ ->
                if maxPendingChunkedMessage > 0 && (pendingChunkedMessageUuidQueue.Count + 1) > maxPendingChunkedMessage then
                    removeOldestPendingChunkedMessage()
            pendingChunkedMessageUuidQueue.AddLast(metadata.Uuid) |> ignore
            let ctx = ChunkedMessageCtx(metadata.NumChunks, metadata.TotalChunkMsgSize)
            chunkedMessagesMap[metadata.Uuid] <- ctx
            Ok ctx
        else
            match chunkedMessagesMap.TryGetValue(metadata.Uuid) with
            | true, ctx ->
                if metadata.NumChunks <> ctx.TotalChunksCount || metadata.TotalChunkMsgSize <> ctx.TotalChunksSize
                   || metadata.ChunkId <> ctx.LastChunkId + %1 || %metadata.ChunkId >= ctx.TotalChunksCount then
                    let error = $"Received unexpected chunk uuid = {metadata.Uuid}, last-chunk-id = {ctx.LastChunkId}, chunkId = {metadata.ChunkId}, total-chunks = {metadata.NumChunks}, expected-total-chunks = {ctx.TotalChunksCount}, total-chunk-msg-size = {metadata.TotalChunkMsgSize}, expected-total-chunk-msg-size = {ctx.TotalChunksSize}"
                    // the chunks received so far go to the unacked tracker, the caller deals with the current chunk
                    pendingChunkedMessageUuidQueue.Remove(metadata.Uuid) |> ignore
                    removeChunkMessage metadata.Uuid ctx false
                    Error error
                else
                    Ok ctx
            | _ ->
                Error <| $"Received unexpected chunk uuid = %A{metadata.Uuid}, chunkId = %A{metadata.ChunkId}, total-chunks = %A{metadata.NumChunks}"
    member this.MessageReceived (rawMessage: RawMessage, msgId: MessageId, ctx: ChunkedMessageCtx, codec: ICompressionCodec) =
        let payloadLength = int rawMessage.Payload.Length
        if ctx.CurrentBufferLength + payloadLength > ctx.TotalChunksSize then
            // the chunks add up to more than the declared size, the message is malformed and would overrun the buffer
            Log.Logger.LogWarning("{0} Discarding chunked message uuid = {1}, chunkId = {2} with {3} bytes exceeds total-chunk-msg-size = {4} after {5} bytes, msgId = {6}",
                                  prefix, rawMessage.Metadata.Uuid, rawMessage.Metadata.ChunkId, payloadLength, ctx.TotalChunksSize, ctx.CurrentBufferLength, msgId)
            // the chunks received so far and the current chunk go to the unacked tracker
            pendingChunkedMessageUuidQueue.Remove rawMessage.Metadata.Uuid |> ignore
            removeChunkMessage rawMessage.Metadata.Uuid ctx false
            ackOrTrack msgId false
            None
        else
            ctx.MessageReceived rawMessage
            // if final chunk is not received yet then release payload and return
            if %rawMessage.Metadata.ChunkId = rawMessage.Metadata.NumChunks - 1 then
                chunkedMessagesMap.Remove rawMessage.Metadata.Uuid |> ignore
                pendingChunkedMessageUuidQueue.Remove rawMessage.Metadata.Uuid |> ignore
                let decompressedPayload = ctx.Decompress(rawMessage.Metadata.UncompressedMessageSize, codec)
                let chunkMsgIds = Some ctx.ChunkedMessageIds
                ctx.Dispose()
                Some (decompressedPayload, { msgId with ChunkMessageIds = chunkMsgIds } )
            else
                None

    member this.RemoveExpireIncompleteChunkedMessages() =
        if pendingChunkedMessageUuidQueue.Count > 0 then
            let firstMsgUuid = pendingChunkedMessageUuidQueue.First.Value
            match chunkedMessagesMap.TryGetValue firstMsgUuid with
            | true, ctx ->
                if Stopwatch.GetElapsedTime(ctx.ReceivedTime) > expireTimeOfIncompleteChunkedMessage then
                    Log.Logger.LogWarning("{0} RemoveExpireIncompleteChunkedMessages {1}", prefix, firstMsgUuid)
                    pendingChunkedMessageUuidQueue.RemoveFirst()
                    removeChunkMessage firstMsgUuid ctx true
                    this.RemoveExpireIncompleteChunkedMessages()
            | _ ->
                pendingChunkedMessageUuidQueue.RemoveFirst()
                this.RemoveExpireIncompleteChunkedMessages()