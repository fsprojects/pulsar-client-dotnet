module Pulsar.Client.UnitTests.Internal.CompressionCodecTests

open System.IO
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Expecto
open Expecto.Flip
open System.Text

[<Tests>]
let tests =

    let createCodec codecType = codecType |> CompressionCodec.get

    let encoding = Encoding.UTF8

    let getBytes (message : string) = message |> encoding.GetBytes

    let getString (bytes : byte[]) = bytes |> encoding.GetString

    let hello = "Hello"
    let helloNone = hello |> getBytes
    let helloZLib = System.Convert.FromBase64String "eJzySM3JyQcAAAD//w=="
    let helloLZ4 = System.Convert.FromBase64String "UEhlbGxv"
    let helloSnappy = System.Convert.FromBase64String "BRBIZWxsbw=="
    let helloZStd = System.Convert.FromBase64String "KLUv/SAFKQAASGVsbG8="
    let helloWorldZstdWithoutDecompressedContentSizeInPayload = System.Convert.FromBase64String "KLUv/QBQ/AAAuGhlbGxvIHdvcmxkIGxvcmVtIGlwc3VtAQDN/zWfTUwAAAhpAQD8/zkQAkwAAAhvAQD8/zkQAkwAAAhyAQD8/zkQAkwAAAhsAQD8/zkQAkwAAAh1AQD8/zkQAkwAAAhtAQD8/zkQAkwAAAggAQD8/zkQAkwAAAh3AQD8/zkQAkwAAAhlAQD8/zkQAkwAAAhwAQD8/zkQAkwAAAhyAQD8/zkQAkwAAAhsAQD8/zkQAkwAAAhvAQD8/zkQAkwAAAhtAQD8/zkQAkwAAAggAQD8/zkQAkUAAAhsAQDkKyAE"

    let testEncode compressionType expectedBytes =
        let codec = compressionType |> createCodec
        let ms = new MemoryStream(helloNone, 0, helloNone.Length, true, true)
        let encoded = ms |> codec.Encode |> _.ToArray()
        encoded |> Expect.sequenceEqual "" expectedBytes

    let testDecode compressionType encodedBytes =
        let uncompressedSize = helloNone.Length
        let codec = compressionType |> createCodec
        let ms = new MemoryStream(encodedBytes, 0, encodedBytes.Length, true, true)
        let decoded = codec.Decode(uncompressedSize, ms) |> _.ToArray() |> getString
        decoded |> Expect.equal "" hello

    testList "CompressionCodec" [

        test "None encoding returns same data" {
            helloNone |> testEncode CompressionType.None
        }

        test "None decoding returns same data" {
            helloNone |> testDecode CompressionType.None
        }

        test "Codec should make ZLib encoding" {
            helloZLib |> testEncode CompressionType.ZLib
        }

        test "Codec should make ZLib decoding" {
            helloZLib |> testDecode CompressionType.ZLib
        }

        test "Codec should make LZ4 encoding" {
            helloLZ4 |> testEncode CompressionType.LZ4
        }

        test "Codec should make LZ4 decoding" {
            helloLZ4 |> testDecode CompressionType.LZ4
        }

        test "Codec should make Snappy encoding" {
            helloSnappy |> testEncode CompressionType.Snappy
        }

        test "Codec should make Snappy decoding" {
            helloSnappy |> testDecode CompressionType.Snappy
        }

        test "Codec should make ZStd encoding" {
            helloZStd |> testEncode CompressionType.ZStd
        }

        test "Codec should make ZStd decoding" {
            helloZStd |> testDecode CompressionType.ZStd
        }

        test "Zstd should decode content with non specififed size in the compressed payload" {
          // The compressed string above has been compressed with zstd using this library: https://www.npmjs.com/package/zstd-codec@0.1.4
          // It seems this library does not specify the decompressed size in the payload.
          //
          // The raw string has been built in JS with the snippet below and then compressed afterwards.
          // const bytes = 1024 * 1024 * 2 + 1000
          // let str = "";
          // while (str.length < bytes)
          //   str += "hello world lorem ipsum"

          let uncompressedSize = 1024 * 1024 * 2 + 1000
          let codec = CompressionType.ZStd |> createCodec
          let ms = new MemoryStream(helloWorldZstdWithoutDecompressedContentSizeInPayload, 0, helloWorldZstdWithoutDecompressedContentSizeInPayload.Length, true, true)
          let decoded = codec.Decode(uncompressedSize, ms) |> _.ToArray() |> getString
          decoded |> Expect.stringStarts "" "hello world lorem ipsum"
        }
    ]