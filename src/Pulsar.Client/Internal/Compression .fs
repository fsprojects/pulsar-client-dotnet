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
    abstract member Encode: src:byte[] -> byte[]
    abstract member Decode: uncompressedSize:int * src:byte[] -> byte[]
    abstract member Decode: uncompressedSize:int * src:byte[] * payloadLength: int -> byte[]

module internal CompressionCodec =

    type ZLibCompression() =
        
        let zlib isEncode (capacity: int) (bytes : byte[]) payloadLength =
            use ms = MemoryStreamManager.GetStream(null, capacity)
            use zlib =
                if isEncode then
                    new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
                else
                    new ZOutputStream(ms)
            zlib.FlushMode <- zlibConst.Z_SYNC_FLUSH
            zlib.Write(bytes, 0, payloadLength)
            ms.ToArray()
        
        interface ICompressionCodec with
            member this.Encode src = zlib true 0 src src.Length
            member this.Decode (uncompressedSize, src) = zlib false uncompressedSize src src.Length
            member this.Decode (uncompressedSize, src, payloadLength) = zlib false uncompressedSize src payloadLength

    type LZ4Compression() =
        interface ICompressionCodec with
            member this.Encode bytes =
                let target = LZ4Codec.MaximumOutputSize bytes.Length |> ArrayPool.Shared.Rent
                let count = LZ4Codec.Encode(bytes, 0, bytes.Length, target, 0, target.Length)
                target |> Array.take count
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
            member this.Encode bytes =
                SnappyCodec.Compress bytes
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
            member this.Encode bytes =
                use zstdCompressor = new Compressor()
                zstdCompressor.Wrap bytes
            member this.Decode (uncompressedSize, bytes) =
                use zstdDecompressor = new Decompressor()
                zstdDecompressor.Unwrap(bytes, uncompressedSize)
            member this.Decode (uncompressedSize, bytes, payloadLength) =
                use zstdDecompressor = new Decompressor()
                zstdDecompressor.Unwrap(new ArraySegment<byte>(bytes, 0, payloadLength), uncompressedSize)
    
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