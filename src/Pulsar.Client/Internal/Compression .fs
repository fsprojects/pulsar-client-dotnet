namespace Pulsar.Client.Internal

open System.Buffers
open System.IO
open Pulsar.Client.Api
open Pulsar.Client.Common
open ComponentAce.Compression.Libs.zlib
open K4os.Compression.LZ4
open Snappy
open ZstdNet

type internal CompressionCodec =
    { Encode: byte[] -> byte[]
      Decode: int -> byte[] -> byte[] }

module internal CompressionCodec =

    let private zlib isEncode (capacity : int) (bytes : byte[]) =
        use ms = MemoryStreamManager.GetStream(null, capacity)
        use zlib =
            if isEncode then
                new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
            else
                new ZOutputStream(ms)
        zlib.Write(bytes, 0, bytes.Length)
        zlib.finish()
        ms.ToArray()

    let private encodeZLib = zlib true 0
    let private decodeZLib = zlib false

    let private encodeLZ4 (bytes : byte[]) =
        let target = ArrayPool.Shared.Rent (LZ4Codec.MaximumOutputSize bytes.Length)
        let count = LZ4Codec.Encode(bytes, 0, bytes.Length, target, 0, target.Length)
        target |> Array.take count

    let private decodeLZ4 (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        LZ4Codec.Decode(bytes, 0, bytes.Length, target, 0, target.Length) |> ignore
        target

    let private encodeSnappy = SnappyCodec.Compress

    let private decodeSnappy (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        SnappyCodec.Uncompress(bytes, 0, bytes.Length, target, 0) |> ignore
        target

    let private encodeZStd () =
        let zstd = new Compressor()
        fun (bytes: byte[]) ->
            bytes |> zstd.Wrap

    let private decodeZStd () =
        let zstd = new Decompressor()
        fun (uncompressedSize : int) (bytes : byte[]) ->
            zstd.Unwrap(bytes, uncompressedSize)

    let create = function
        | CompressionType.ZLib -> { Encode = encodeZLib; Decode = decodeZLib }
        | CompressionType.LZ4 -> { Encode = encodeLZ4; Decode = decodeLZ4 }
        | CompressionType.Snappy -> { Encode = encodeSnappy; Decode = decodeSnappy }
        | CompressionType.ZStd -> { Encode = encodeZStd(); Decode = decodeZStd() }
        | CompressionType.None -> { Encode = id; Decode = fun _ bytes -> bytes }
        | _ as unknown -> raise(NotSupportedException <| sprintf "Compression codec '%A' not supported." unknown)