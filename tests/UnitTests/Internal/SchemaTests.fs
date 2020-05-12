module Pulsar.Client.UnitTests.Internal.SchemaTests

open System
open System.Collections.Generic
open Expecto
open Expecto.Flip
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Schema
open Pulsar.Client.UnitTests

[<CLIMutable>]
type JsonSchemaTest = { X: string; Y: ResizeArray<int> }

[<CLIMutable>]
[<ProtoContract>]
type ProtobufchemaTest = {
        [<ProtoMember(1)>]X: string
        [<ProtoMember(2)>]Y: ResizeArray<int>
    }

[<CLIMutable>]
type AvroSchemaTest = { X: string; Y: ResizeArray<int> }

[<Tests>]
let tests =
    
    ftestList "Schema tests" [

        test "Bytes schema works fine" {
            let input = [| 1uy; 2uy; 3uy |]
            let schema = Schema.BYTES()
            let output =
                input
                |> schema.Encode
                |> schema.Decode
            Expect.equal "" input output
        }
        
        test "Bool schema works fine" {
            let inputs = [true; false]
            for input in inputs do
                let schema = Schema.BOOL()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Date schema works fine" {
            let inputs = [DateTime.Now; DateTime.Now]
            for input in inputs do
                let schema = Schema.DATE()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.isLessThan "" ((input - output),(TimeSpan.FromMilliseconds(1.0)))
        }
        
        test "Time schema works fine" {
            let inputs = [TimeSpan.Zero; TimeSpan.FromMilliseconds(1.0); TimeSpan.FromDays(1000.0)]
            for input in inputs do
                let schema = Schema.TIME()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.isLessThan "" ((input - output),(TimeSpan.FromMilliseconds(1.0)))
        }
        
        test "Timestamp schema works fine" {
            let inputs = [DateTimeOffset.Now; DateTimeOffset.UtcNow]
            for input in inputs do
                let schema = Schema.TIMESTAMP()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.isLessThan "" ((input - output),(TimeSpan.FromMilliseconds(1.0)))
        }
        
        test "Double schema works fine" {
            let inputs = [0.0; Double.MaxValue; Double.MinValue]
            for input in inputs do
                let schema = Schema.DOUBLE()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Float schema works fine" {
            let inputs = [0.0f; Single.MaxValue; Single.MinValue]
            for input in inputs do
                let schema = Schema.FLOAT()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Byte schema works fine" {
            let inputs = [0uy; Byte.MaxValue; Byte.MinValue]
            for input in inputs do
                let schema = Schema.INT8()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Short schema works fine" {
            let inputs = [0s; Int16.MaxValue; Int16.MinValue]
            for input in inputs do
                let schema = Schema.INT16()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Integer schema works fine" {
            let inputs = [0; Int32.MaxValue; Int32.MinValue]
            for input in inputs do
                let schema = Schema.INT32()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "Long schema works fine" {
            let inputs = [0L; Int64.MaxValue; Int64.MinValue]
            for input in inputs do
                let schema = Schema.INT64()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "String schema works fine" {
            let inputs = [String.Empty; "abcd"]
            for input in inputs do
                let schema = Schema.STRING()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input output
        }
        
        test "JSON schema works fine" {
            let inputs = [{ JsonSchemaTest.X = "X1"; Y= seq { 1; 2 } |> ResizeArray}]
            for input in inputs do
                let schema = Schema.JSON()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.X output.X
                Expect.sequenceEqual "" input.Y output.Y
        }
        
        test "Avro schema works fine" {
            let inputs = [{ AvroSchemaTest.X = "X1"; Y = seq { 1; 2 } |> ResizeArray}]
            for input in inputs do
                let schema = Schema.AVRO()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.X output.X
                Expect.sequenceEqual "" input.Y output.Y
        }
        
        test "Protobuf schema works fine" {
            let inputs = [{ ProtobufchemaTest.X = "X1"; Y = seq { 1; 2 } |> ResizeArray}]
            for input in inputs do
                let schema = Schema.PROTOBUF()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.X output.X
                Expect.sequenceEqual "" input.Y output.Y
        }
        
        test "KeyValue schema works fine" {
            let inputs = [KeyValuePair(10, KeyValuePair(1uy, "aabb")); KeyValuePair(-1, KeyValuePair(0uy, ""))]
            for input in inputs do
                let schema = Schema.KEY_VALUE(Schema.INT32(), Schema.KEY_VALUE(
                                                  Schema.INT8(), Schema.STRING(Text.Encoding.ASCII), KeyValueEncodingType.INLINE
                                              ), KeyValueEncodingType.SEPARATED)
                let processor = KeyValueProcessor.GetInstance(schema)
                let output =
                    input
                    |> processor.Value.EncodeKeyValue
                    |> (fun struct(k, v) -> k, v)
                    |> processor.Value.DecodeKeyValue
                    |> unbox
                Expect.equal "" input output
        }
    ]
