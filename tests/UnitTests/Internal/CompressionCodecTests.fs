module Pulsar.Client.UnitTests.Internal.CompressionCodecTests

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

    let testEncode compressionType expectedBytes =
        let codec = compressionType |> createCodec
        let encoded = hello |> getBytes |> codec.Encode
        Expect.isTrue "" (encoded = expectedBytes)

    let testDecode compressionType encodedBytes =
        let uncompressedSize = helloNone.Length
        let codec = compressionType |> createCodec
        let decoded = codec.Decode(uncompressedSize, encodedBytes) |> getString
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
    ]