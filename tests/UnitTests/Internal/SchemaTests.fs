module Pulsar.Client.UnitTests.Internal.SchemaTests

open System
open System.Collections.Generic
open System.Diagnostics
open AvroGenerated
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
type ProtobufSchemaTest = {
        [<ProtoMember(1)>]X: string
        [<ProtoMember(2)>]Y: ResizeArray<int>
    }

[<CLIMutable>]
type AvroSchemaTest = { X: string; Y: ResizeArray<int> }

[<CLIMutable>]
[<ProtoContract>]
type ProtobufNativeSchemaTest = {
        [<ProtoMember(1)>]foo: string
        [<ProtoMember(2)>]bar: double
        [<ProtoMember(3)>][<CompatibilityLevel(CompatibilityLevel.Level300)>]time: DateTime
    }

[<Tests>]
let tests =
    
    testList "Schema tests" [

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
        
        test "Protobuf native" {
            let inputs = [{ ProtobufNativeSchemaTest.foo = "X1"; bar = 1.0; time = DateTime.Now}]
            for input in inputs do
                let schema = Schema.PROTOBUF_NATIVE<ProtobufNativeSchemaTest>()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.foo output.foo
                Expect.equal "" input.bar output.bar
                Expect.equal "" input.time output.time
        }
        
        test "Avro schema works fine with Avro generated classes" {
            let inputs = [ SampleClass(
                                          value1 = 1L,
                                          value2 = "some string",
                                          value3 = SampleEnum.S1,
                                          value4 = List(["some string list"]),
                                          value5 = SampleNestedClass( a = 1L, b = "s")
                                      ) ]
            for input in inputs do
                let schema = Schema.AVRO<SampleClass>()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.value1 output.value1
                Expect.equal "" input.value2 output.value2
                Expect.equal "" input.value3 output.value3
                Expect.sequenceEqual "" input.value4 output.value4
                Expect.equal "" input.value5.a output.value5.a
                Expect.equal "" input.value5.b output.value5.b
        }

        test "Protobuf native schema works fine with generated classes" {
            let inputs = [ SearchRequest(
                                    Query = "Sample query",
                                    PageNumber = 10,
                                    ResultPerPage = 20,
                                    corpus = SearchRequest.Corpus.Images
                                        )]
            for input in inputs do
                let schema = Schema.PROTOBUF_NATIVE<SearchRequest>()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.Query output.Query
                Expect.equal "" input.PageNumber output.PageNumber
                Expect.equal "" input.ResultPerPage output.ResultPerPage
                Expect.equal "" input.corpus output.corpus               
        }

        test "Avro schema works fine with long strings (> 256 characters)" {
            let inputs = [{ AvroSchemaTest.X = String('1', 257); Y = [] |> ResizeArray}]
            for input in inputs do
                let schema = Schema.AVRO()
                let output =
                    input
                    |> schema.Encode
                    |> schema.Decode
                Expect.equal "" input.X output.X
        }

        test "Protobuf schema works fine" {
            let inputs = [{ ProtobufSchemaTest.X = "X1"; Y = seq { 1; 2 } |> ResizeArray}]
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
        
        ptest "Serialize schema perf" {
            let inputs = [{ JsonSchemaTest.X = "X1"; Y= seq { 1; 2 } |> ResizeArray}]
            let sw = Stopwatch()
            sw.Start()
            let jsSchema = Schema.JSON<JsonSchemaTest>()
            let avroSchema = Schema.AVRO<JsonSchemaTest>()
            for i in 1..10000 do
                for input in inputs do
                    input
                    |> jsSchema.Encode
                    |> ignore
            let jsonTime = sw.Elapsed.TotalSeconds
            sw.Restart()
            for i in 1..10000 do
                for input in inputs do
                    input
                    |> avroSchema.Encode
                    |> ignore
            sw.Stop()
            let avroTime = sw.Elapsed.TotalSeconds
            Console.WriteLine("Encode Json time: {0} Avro time: {1}", jsonTime, avroTime)
        }
        
        ptest "Deserialize schema perf" {
            let inputs = [{ JsonSchemaTest.X = "X1"; Y= seq { 1; 2 } |> ResizeArray}]
            let sw = Stopwatch()
            sw.Start()
            let jsSchema = Schema.JSON<JsonSchemaTest>()
            let avroSchema = Schema.AVRO<JsonSchemaTest>()
            let jsInput = jsSchema.Encode(inputs.[0])
            let avroInput = avroSchema.Encode(inputs.[0])
            
            for i in 1..100000 do
                    jsInput
                    |> jsSchema.Decode
                    |> ignore
            let jsonTime = sw.Elapsed.TotalSeconds
            sw.Restart()
            for i in 1..100000 do
                    avroInput
                    |> avroSchema.Decode
                    |> ignore
            sw.Stop()
            let avroTime = sw.Elapsed.TotalSeconds
            Console.WriteLine("Decode Json time: {0} Avro time: {1}", jsonTime, avroTime)
        }
    ]
