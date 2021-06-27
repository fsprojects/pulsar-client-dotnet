namespace Pulsar.Client.Internal

open System
open System.Buffers
open System.Collections.Generic
open Pulsar.Client.Api
open Pulsar.Client.Common
open FSharp.UMX
open Microsoft.Extensions.Logging

type internal ChunkedMessageCtx(totalChunksCount: int, totalChunksSize: int) =
    let chunkedMessageIds = Array.zeroCreate totalChunksCount
    let chunkedMsgBuffer = ArrayPool.Shared.Rent totalChunksSize
    let receivedTime = DateTime.Now
    let mutable lastChunkId: ChunkId = %(-1)
    let mutable currentBufferLength = 0
        
    with
        member this.MessageReceived(msg: RawMessage) =
            // append the chunked payload and update lastChunkedMessage-id
            chunkedMessageIds.[%msg.Metadata.ChunkId] <- msg.MessageId
            msg.Payload.CopyTo(chunkedMsgBuffer, currentBufferLength)
            currentBufferLength <- currentBufferLength + msg.Payload.Length
            lastChunkId <- msg.Metadata.ChunkId
        member this.LastChunkId = lastChunkId
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
        let ctx = chunkedMessagesMap.[firstPendingMsgUuid]
        pendingChunkedMessageUuidQueue.RemoveFirst()
        removeChunkMessage firstPendingMsgUuid ctx autoAckOldestChunkedMessageOnQueueFull
        
    member this.GetContext (metadata: Metadata) =
        if metadata.ChunkId = %0 then
            let ctx = ChunkedMessageCtx(metadata.NumChunks, metadata.TotalChunkMsgSize)
            chunkedMessagesMap.[metadata.Uuid] <- ctx
            if maxPendingChunkedMessage > 0 && (pendingChunkedMessageUuidQueue.Count + 1) > maxPendingChunkedMessage then
                removeOldestPendingChunkedMessage()
            pendingChunkedMessageUuidQueue.AddLast(metadata.Uuid) |> ignore
            Ok ctx
        else
            match chunkedMessagesMap.TryGetValue(metadata.Uuid) with
            | true, ctx ->
                if metadata.ChunkId <> ctx.LastChunkId + %1 || %metadata.ChunkId > metadata.NumChunks then
                    ctx.Dispose()
                    chunkedMessagesMap.Remove(metadata.Uuid) |> ignore
                    Error <| $"Received unexpected chunk uuid = {metadata.Uuid}, last-chunk-id = {ctx.LastChunkId}, chunkId = {metadata.ChunkId}, total-chunks = {metadata.NumChunks}"
                else
                    Ok ctx
            | _ ->
                Error <| $"Received unexpected chunk uuid = %A{metadata.Uuid}, chunkId = %A{metadata.ChunkId}, total-chunks = %A{metadata.NumChunks}"
    member this.MessageReceived (rawMessage, msgId: MessageId, ctx: ChunkedMessageCtx, codec: ICompressionCodec) =
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
            let ctx = chunkedMessagesMap.[firstMsgUuid]
            if DateTime.Now > ctx.ReceivedTime.Add(expireTimeOfIncompleteChunkedMessage) then
                Log.Logger.LogWarning("{0} RemoveExpireIncompleteChunkedMessages {1}", prefix, pendingChunkedMessageUuidQueue.First.Value)
                pendingChunkedMessageUuidQueue.RemoveFirst()
                removeChunkMessage firstMsgUuid ctx true
                this.RemoveExpireIncompleteChunkedMessages()