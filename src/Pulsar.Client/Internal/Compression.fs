namespace Pulsar.Client.Internal

open System
open System.Buffers
open System.IO
open Pulsar.Client.Api
open Pulsar.Client.Common
open ComponentAce.Compression.Libs.zlib
open K4os.Compression.LZ4
open Snappy
open ZstdNet

type ICompressionCodec =
    abstract member Encode: src:MemoryStream -> MemoryStream
    abstract member Decode: uncompressedSize:int * src:MemoryStream -> MemoryStream
    abstract member Decode: uncompressedSize:int * src:byte[] * payloadLength: int -> byte[]

module internal CompressionCodec =

    type ZLibCompression() =
        let zlibEncode (payload: MemoryStream) =
            let ms = MemoryStreamManager.GetStream()
            let zlib = new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(payload.ToArray(), 0, int payload.Length)
            payload.Dispose()
            ms

        let zlibDecode (uncompressedSize: int) (payload: MemoryStream) =
            let ms = MemoryStreamManager.GetStream(null, uncompressedSize)
            let zlib = new ZOutputStream(ms)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(payload.ToArray(), 0, int payload.Length)
            payload.Dispose()
            ms

        let zlibDecodeBytes (uncompressedSize: int) (bytes : byte[]) payloadLength =
            use ms = MemoryStreamManager.GetStream(null, uncompressedSize)
            use zlib = new ZOutputStream(ms)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(bytes, 0, payloadLength)
            ms.ToArray()

        interface ICompressionCodec with
            member this.Encode src = zlibEncode src
            member this.Decode (uncompressedSize, src) = zlibDecode uncompressedSize src
            member this.Decode (uncompressedSize, src, payloadLength) = zlibDecodeBytes uncompressedSize src payloadLength

    type LZ4Compression() =
        interface ICompressionCodec with
            member this.Encode payload =
                let target = LZ4Codec.MaximumOutputSize (int payload.Length) |> ArrayPool.Shared.Rent
                try
                    let sourceSpan = payload.ToArray().AsSpan()
                    let targetSpan = target.AsSpan()
                    let count = LZ4Codec.Encode(sourceSpan, targetSpan)
                    let ms = MemoryStreamManager.GetStream()
                    ms.Write(targetSpan.Slice(0, count))
                    ms
                finally
                    payload.Dispose()
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, payload) =
                let target: byte[] = uncompressedSize |> ArrayPool.Shared.Rent
                try
                    LZ4Codec.Decode(payload.ToArray(), 0, int payload.Length, target, 0, uncompressedSize) |> ignore
                    let ms = MemoryStreamManager.GetStream(null, uncompressedSize)
                    ms.Write(target, 0, uncompressedSize)
                    ms
                finally
                    payload.Dispose()
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                let target = Array.zeroCreate uncompressedSize
                LZ4Codec.Decode(bytes, 0, payloadLength, target, 0, target.Length) |> ignore
                target

    type SnappyCompression() =
        interface ICompressionCodec with
            member this.Encode payload =
                let length = (int payload.Length)
                let source: byte[] = length |> ArrayPool.Shared.Rent
                let target: byte[] = SnappyCodec.GetMaxCompressedLength length |> ArrayPool.Shared.Rent
                try
                    let sourceSpan = payload.ToArray().AsSpan()
                    sourceSpan.CopyTo(source)
                    let count = SnappyCodec.Compress(source, 0, length, target, 0)
                    let ms = MemoryStreamManager.GetStream()
                    ms.Write(target.AsSpan(0, count))
                    ms
                finally
                    payload.Dispose()
                    source |> ArrayPool.Shared.Return
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, payload) =
                let target: byte[] = uncompressedSize |> ArrayPool.Shared.Rent
                try
                    SnappyCodec.Uncompress(payload.ToArray(), 0, int payload.Length, target, 0) |> ignore
                    let ms = MemoryStreamManager.GetStream(null, uncompressedSize)
                    ms.Write(target, 0, uncompressedSize)
                    ms
                finally
                    payload.Dispose()
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                let target = Array.zeroCreate uncompressedSize
                SnappyCodec.Uncompress(bytes, 0, payloadLength, target, 0) |> ignore
                target

    type ZstdCompression() =
        interface ICompressionCodec with
            member this.Encode payload =
                let target: byte[] = Compressor.GetCompressBound(int payload.Length) |> ArrayPool.Shared.Rent
                let zstdCompressor = new Compressor()
                try
                    let sourceSpan = payload.ToArray().AsSpan()
                    let count = zstdCompressor.Wrap(sourceSpan, target)
                    let ms = MemoryStreamManager.GetStream()
                    ms.Write(target.AsSpan(0, count))
                    ms
                finally
                    zstdCompressor.Dispose()
                    payload.Dispose()
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, payload) =
                let target: byte[] = uncompressedSize |> ArrayPool.Shared.Rent
                let zstdDecompressor = new Decompressor()
                try
                    let sourceSpan = payload.ToArray().AsSpan()
                    zstdDecompressor.Unwrap(sourceSpan, target, false) |> ignore
                    let ms = MemoryStreamManager.GetStream(null, uncompressedSize)
                    ms.Write(target, 0, uncompressedSize)
                    ms
                finally
                    zstdDecompressor.Dispose()
                    payload.Dispose()
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                use zstdDecompressor = new Decompressor()
                zstdDecompressor.Unwrap(ArraySegment<byte>(bytes, 0, payloadLength), uncompressedSize)

    let zlibCompression = lazy(ZLibCompression() :> ICompressionCodec)
    let lz4Compression = lazy(LZ4Compression() :> ICompressionCodec)
    let snappyCompression = lazy(SnappyCompression() :> ICompressionCodec)
    let zstdCompression = lazy(ZstdCompression() :> ICompressionCodec)
    let noCompression = lazy {
        new ICompressionCodec with
            member this.Encode bytes = bytes
            member this.Decode (uncompressedSize, bytes) = bytes
            member this.Decode (uncompressedSize, bytes, payloadLength) =  bytes |> Array.take payloadLength
    }


    let get = function
        | CompressionType.ZLib -> zlibCompression.Value
        | CompressionType.LZ4 -> lz4Compression.Value
        | CompressionType.Snappy -> snappyCompression.Value
        | CompressionType.ZStd -> zstdCompression.Value
        | CompressionType.None -> noCompression.Value
        | unknown -> raise(NotSupportedException <| sprintf "Compression codec '%A' not supported." unknown)