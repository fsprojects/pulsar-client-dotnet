namespace Pulsar.Client.Internal

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

    let private zlib (bytes : byte[]) (createZLibStream : Stream -> ZOutputStream) (capacity : int) =
        use ms = new MemoryStream(capacity)
        use zlib = createZLibStream ms
        zlib.Write(bytes, 0, bytes.Length)
        zlib.finish()
        ms.ToArray()

    let private encodeZLib (bytes : byte[]) =
        let createZLibStream ms = new ZOutputStream(ms, zlibConst.Z_DEFAULT_COMPRESSION)
        createZLibStream |> zlib bytes <| 0

    let private decodeZLib (uncompressedSize : int) (bytes : byte[]) =
        let createZLibStream ms = new ZOutputStream(ms)
        createZLibStream |> zlib bytes <| uncompressedSize

    let private encodeLZ4 (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(LZ4Codec.MaximumOutputSize(bytes.Length))
        let count = LZ4Codec.Encode(bytes, 0, bytes.Length, target, 0, target.Length)
        target |> Array.take count

    let private decodeLZ4 (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        LZ4Codec.Decode(bytes, 0, bytes.Length, target, 0, target.Length) |> ignore
        target

    let private encodeSnappy (bytes : byte[]) =
        bytes |> SnappyCodec.Compress

    let private decodeSnappy (uncompressedSize : int) (bytes : byte[]) =
        let target = Array.zeroCreate<byte>(uncompressedSize)
        SnappyCodec.Uncompress(bytes, 0, bytes.Length, target, 0) |> ignore
        target

    let private encodeZStd (bytes : byte[]) =
        use zstd = new Compressor()
        bytes |> zstd.Wrap

    let private decodeZStd (uncompressedSize : int) (bytes : byte[]) =
        use zstd = new Decompressor()
        zstd.Unwrap(bytes, uncompressedSize)

    let create = function
        | CompressionType.ZLib -> { Encode = encodeZLib; Decode = decodeZLib }
        | CompressionType.LZ4 -> { Encode = encodeLZ4; Decode = decodeLZ4 }
        | CompressionType.Snappy -> { Encode = encodeSnappy; Decode = decodeSnappy }
        | CompressionType.ZStd -> { Encode = encodeZStd; Decode = decodeZStd }
        | CompressionType.None -> { Encode = id; Decode = fun _ bytes -> bytes }
        | _ as unknown -> raise(NotSupportedException <| sprintf "Compression codec '%A' not supported." unknown)