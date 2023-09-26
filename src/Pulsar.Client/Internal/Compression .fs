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
    abstract member Decode: uncompressedSize:int * src:byte[] -> byte[]
    abstract member Decode: uncompressedSize:int * src:byte[] * payloadLength: int -> byte[]

module internal CompressionCodec =

    type ZLibCompression() =
        let zlibEncode (capacity: int) (bytes: MemoryStream) payloadLength =
            let ms = new MemoryStream(capacity)
            let zlib = new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(bytes.GetBuffer(), 0, payloadLength)
            ms

        let zlibDecode (capacity: int) (bytes : byte[]) payloadLength =
            use ms = MemoryStreamManager.GetStream(null, capacity)
            use zlib = new ZOutputStream(ms)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(bytes, 0, payloadLength)
            ms.ToArray()

        interface ICompressionCodec with
            member this.Encode src = zlibEncode 0 src (int src.Length)
            member this.Decode (uncompressedSize, src) = zlibDecode uncompressedSize src src.Length
            member this.Decode (uncompressedSize, src, payloadLength) = zlibDecode uncompressedSize src payloadLength

    type LZ4Compression() =
        interface ICompressionCodec with
            member this.Encode stream =
                let target = LZ4Codec.MaximumOutputSize (int stream.Length) |> ArrayPool.Shared.Rent
                try
                    let sourceSpan = getSpan stream
                    let count = LZ4Codec.Encode(sourceSpan, target.AsSpan())
                    new MemoryStream(target |> Array.take count)
                finally
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, bytes) =
                let target = Array.zeroCreate uncompressedSize
                LZ4Codec.Decode(bytes, 0, bytes.Length, target, 0, target.Length) |> ignore
                target
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                let target = Array.zeroCreate uncompressedSize
                LZ4Codec.Decode(bytes, 0, payloadLength, target, 0, target.Length) |> ignore
                target

    type SnappyCompression() =
        interface ICompressionCodec with
            member this.Encode stream =
                let length = (int stream.Length)
                let source: byte[] = length |> ArrayPool.Shared.Rent
                let target: byte[] = SnappyCodec.GetMaxCompressedLength length |> ArrayPool.Shared.Rent
                try
                    let sourceSpan = getSpan stream
                    sourceSpan.CopyTo(source)
                    let count = SnappyCodec.Compress(source, 0, length, target, 0)
                    new MemoryStream(target |> Array.take count)
                finally
                    source |> ArrayPool.Shared.Return
                    target |> ArrayPool.Shared.Return
            member this.Decode (uncompressedSize, bytes) =
                let target = Array.zeroCreate uncompressedSize
                SnappyCodec.Uncompress(bytes, 0, bytes.Length, target, 0) |> ignore
                target
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                let target = Array.zeroCreate uncompressedSize
                SnappyCodec.Uncompress(bytes, 0, payloadLength, target, 0) |> ignore
                target

    type ZstdCompression() =
        interface ICompressionCodec with
            member this.Encode stream =
                use zstdCompressor = new Compressor()
                let sourceSpan = getSpan stream
                new MemoryStream(zstdCompressor.Wrap sourceSpan)
            member this.Decode (uncompressedSize, bytes) =
                use zstdDecompressor = new Decompressor()
                zstdDecompressor.Unwrap(bytes, uncompressedSize)
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                use zstdDecompressor = new Decompressor()
                zstdDecompressor.Unwrap(ArraySegment<byte>(bytes, 0, payloadLength), uncompressedSize)

    let zlibCompression = lazy(ZLibCompression() :> ICompressionCodec)
    let lz4Compression = lazy(LZ4Compression() :> ICompressionCodec)
    let snappyCompression = lazy(SnappyCompression() :> ICompressionCodec)
    let zstdCompression = lazy(ZstdCompression() :> ICompressionCodec)
    let noCompression = lazy(
                                {
                                    new ICompressionCodec with
                                        member this.Encode bytes = bytes
                                        member this.Decode (uncompressedSize, bytes) = bytes
                                        member this.Decode (uncompressedSize, bytes, payloadLength) =  bytes |> Array.take payloadLength
                                }
                            )

    let get = function
        | CompressionType.ZLib -> zlibCompression.Value
        | CompressionType.LZ4 -> lz4Compression.Value
        | CompressionType.Snappy -> snappyCompression.Value
        | CompressionType.ZStd -> zstdCompression.Value
        | CompressionType.None -> noCompression.Value
        | _ as unknown -> raise(NotSupportedException <| sprintf "Compression codec '%A' not supported." unknown)