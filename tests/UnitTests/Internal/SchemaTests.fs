module Pulsar.Client.UnitTests.Internal.SchemaTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Text
open System.Text.Json
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

[<CLIMutable; AvroSchemaGenerator.Attributes.Aliases("AvroSchemaTest")>]
type AvroSchemaTest2 = { X: string; Y: ResizeArray<int>; Z: string }

[<CLIMutable>]
type DateTimeSchemaTest = { OccurredAt: DateTime }

type TestSchema(schemaInfo, validate) =
    inherit ISchema<string>()

    override this.SchemaInfo = schemaInfo
    override this.Encode value = Encoding.UTF8.GetBytes(value)
    override this.Decode bytes = Encoding.UTF8.GetString(bytes)
    override this.Validate bytes = validate bytes

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

        test "JSON schema DateTime encoding works for all kinds of DateTime values" {
            let schema = Schema.JSON<DateTimeSchemaTest>()
            let inputs = [
                DateTime(2026, 4, 20, 6, 30, 3, DateTimeKind.Local).AddTicks(715180L)
                DateTime(2026, 4, 20, 6, 30, 3, DateTimeKind.Unspecified).AddTicks(715180L)
                DateTime(2026, 4, 20, 6, 30, 3, DateTimeKind.Utc).AddTicks(715180L)
            ]

            for input in inputs do
                let payload = schema.Encode({ OccurredAt = input })
                use doc = JsonDocument.Parse(payload)
                let occuredAtProperty = doc.RootElement.GetProperty("OccurredAt")
                Expect.equal "" JsonValueKind.Number occuredAtProperty.ValueKind
                
                let timestamp = occuredAtProperty.GetInt64()
                let expected =
                    input
                    |> DateTimeOffset
                    |> _.ToUnixTimeMilliseconds()
                Expect.equal "" expected timestamp
        }

        test "JSON schema DateTime decoding accepts numeric timestamp value" {
            let schema = Schema.JSON<DateTimeSchemaTest>()
            let expected = DateTimeOffset.FromUnixTimeMilliseconds(1776666603071L).UtcDateTime
            let payload = Encoding.UTF8.GetBytes("""{"OccurredAt":1776666603071}""")

            let output = schema.Decode(payload)

            Expect.equal "" expected output.OccurredAt
        }

        test "JSON schema DateTime decoding accepts legacy string timestamp value" {
            let schema = Schema.JSON<DateTimeSchemaTest>()
            let expected = DateTime(2026, 4, 20, 6, 30, 3, DateTimeKind.Utc).AddTicks(715180L)
            let payload = Encoding.UTF8.GetBytes("""{"OccurredAt":"2026-04-20T06:30:03.071518Z"}""")

            let output = schema.Decode(payload)

            Expect.equal "" expected output.OccurredAt
        }

        test "Avro schema DateTime encoding writes numeric timestamp value" {
            let schema = Schema.AVRO<DateTimeSchemaTest>()
            let input = { OccurredAt = DateTime(2026, 4, 20, 6, 30, 3, DateTimeKind.Utc).AddTicks(715180L) }
            use stream = new IO.MemoryStream(schema.Encode(input))
            let decoder = Avro.IO.BinaryDecoder(stream)
            let unionBranch = decoder.ReadLong()
            let timestamp = decoder.ReadLong()

            Expect.equal "" 1L unionBranch
            Expect.equal "" (DateTimeOffset(input.OccurredAt).ToUnixTimeMilliseconds()) timestamp
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

        test "Schema-bound auto produce preserves schema info and validates bytes" {
            let properties =
                readOnlyDict [ "source", "reader-schema" ]
            let schemaInfo = {
                Name = "writer"
                Type = SchemaType.JSON
                Schema = Encoding.UTF8.GetBytes("""{"type":"record","name":"Value","fields":[]}""")
                Properties = properties
            }
            let mutable validatedBytes = None
            let sourceSchema =
                TestSchema(schemaInfo, fun bytes -> validatedBytes <- Some bytes)
                :> ISchema<string>
            let autoProduceSchema = Schema.AUTO_PRODUCE(sourceSchema)
            let payload = Encoding.UTF8.GetBytes("{}")

            let encoded = autoProduceSchema.Encode(payload)

            obj.ReferenceEquals(payload, encoded) |> Expect.isTrue ""
            validatedBytes.IsSome |> Expect.isTrue ""
            obj.ReferenceEquals(payload, validatedBytes.Value) |> Expect.isTrue ""
            Expect.equal "" schemaInfo.Name autoProduceSchema.SchemaInfo.Name
            Expect.equal "" schemaInfo.Type autoProduceSchema.SchemaInfo.Type
            Expect.sequenceEqual "" schemaInfo.Schema autoProduceSchema.SchemaInfo.Schema
            obj.ReferenceEquals(schemaInfo.Properties, autoProduceSchema.SchemaInfo.Properties) |> Expect.isTrue ""
        }

        test "Parameterless auto produce remains an unresolved schema stub" {
            let autoProduceSchema = Schema.AUTO_PRODUCE()

            Expect.equal "" SchemaType.AUTO_PUBLISH autoProduceSchema.SchemaInfo.Type
            Expect.throwsT<SchemaSerializationException> "" (fun () -> autoProduceSchema.Encode([||]) |> ignore)
        }

        test "Version-specific Avro schema exposes writer schema info" {
            let readerSchema = Schema.AVRO<AvroSchemaTest>()
            let properties =
                readOnlyDict [ "version", "writer" ]
            let writerSchemaInfo = {
                readerSchema.SchemaInfo with
                    Name = "writer"
                    Properties = properties
            }
            let writerSchema =
                readerSchema.GetSpecificSchema(writerSchemaInfo, Some { Bytes = [| 1uy |] })
            let value = { AvroSchemaTest.X = "X1"; Y = ResizeArray([ 1; 2 ]) }

            Expect.equal "" writerSchemaInfo.Name writerSchema.SchemaInfo.Name
            obj.ReferenceEquals(properties, writerSchema.SchemaInfo.Properties) |> Expect.isTrue ""
            value
            |> readerSchema.Encode
            |> writerSchema.Decode
            |> fun decoded ->
                Expect.equal "" value.X decoded.X
                Expect.sequenceEqual "" value.Y decoded.Y
        }

        test "Version-specific separated KeyValue schema preserves metadata and validates the value payload" {
            let valueSchema = Schema.AVRO<AvroSchemaTest>()
            let readerSchema =
                Schema.KEY_VALUE(Schema.INT32(), valueSchema, KeyValueEncodingType.SEPARATED)
            let writerSchemaInfo = {
                readerSchema.SchemaInfo with
                    Name = "key-value-writer"
            }
            let writerSchema =
                readerSchema.GetSpecificSchema(writerSchemaInfo, Some { Bytes = [| 1uy |] })
            let payload =
                valueSchema.Encode({ AvroSchemaTest.X = "X1"; Y = ResizeArray([ 1; 2 ]) })
            let autoProduceSchema = Schema.AUTO_PRODUCE(writerSchema)

            Expect.equal "" writerSchemaInfo.Name writerSchema.SchemaInfo.Name
            Expect.equal "" SchemaType.AVRO
                (KeyValueSchema.DecodeKeyValueSchemaInfo(writerSchema.SchemaInfo) |> snd).Type
            obj.ReferenceEquals(payload, autoProduceSchema.Encode(payload)) |> Expect.isTrue ""
        }

        test "Version-specific separated KeyValue decoder uses the writer schema" {
            let keySchema = Schema.INT32()
            let writerValueSchema = Schema.AVRO<AvroSchemaTest>()
            let readerValueSchema = Schema.AVRO<AvroSchemaTest2>()
            let readerSchema =
                Schema.KEY_VALUE(keySchema, readerValueSchema, KeyValueEncodingType.SEPARATED)
            let writerSchemaInfo =
                KeyValueSchema.EncodeKeyValueSchemaInfo(
                    "KeyValue",
                    keySchema.SchemaInfo,
                    writerValueSchema.SchemaInfo,
                    KeyValueEncodingType.SEPARATED)
            let specificReaderSchema =
                readerSchema.GetSpecificSchema(writerSchemaInfo, Some { Bytes = [| 1uy |] })
            let decode = KeyValueProcessor.GetDecodeFunction specificReaderSchema
            let writerValue = { AvroSchemaTest.X = "X1"; Y = ResizeArray([ 1; 2 ]) }
            let key = keySchema.Encode(1) |> Convert.ToBase64String
            let payload = writerValueSchema.Encode(writerValue)

            let decoded = decode key payload

            Expect.equal "" 1 decoded.Key
            Expect.equal "" writerValue.X decoded.Value.X
            Expect.sequenceEqual "" writerValue.Y decoded.Value.Y
            Expect.isNull "" decoded.Value.Z
        }

        test "Auto produce validation respects KeyValue encoding" {
            let keySchema = Schema.INT32()
            let valueSchema = Schema.STRING()
            let inlineSchema =
                Schema.KEY_VALUE(keySchema, valueSchema, KeyValueEncodingType.INLINE)
            let separatedSchema =
                Schema.KEY_VALUE(keySchema, valueSchema, KeyValueEncodingType.SEPARATED)
            let inlineTopicSchema = {
                SchemaInfo = inlineSchema.SchemaInfo
                SchemaVersion = None
            }
            let separatedTopicSchema = {
                SchemaInfo = separatedSchema.SchemaInfo
                SchemaVersion = None
            }
            let inlinePayload =
                KeyValueSchema.GetKeyValueBytes(keySchema.Encode(1), valueSchema.Encode("value"))
            let separatedPayload = valueSchema.Encode("value")

            Schema.GetValidateFunction(inlineTopicSchema) inlinePayload
            Schema.GetValidateFunction(separatedTopicSchema) separatedPayload
        }

        test "Structured schemas preserve version-specific schema info" {
            let properties = readOnlyDict [ "version", "writer" ]
            let version: SchemaVersion option = Some { Bytes = [| 1uy |] }
            let assertSchemaInfo expected (actual: SchemaInfo) =
                Expect.equal "" expected.Name actual.Name
                Expect.equal "" expected.Type actual.Type
                Expect.sequenceEqual "" expected.Schema actual.Schema
                obj.ReferenceEquals(expected.Properties, actual.Properties) |> Expect.isTrue ""

            let jsonSchema = Schema.JSON<JsonSchemaTest>()
            let jsonSchemaInfo = {
                jsonSchema.SchemaInfo with
                    Name = "json-writer"
                    Properties = properties
            }
            let specificJsonSchema = jsonSchema.GetSpecificSchema(jsonSchemaInfo, version)
            assertSchemaInfo jsonSchemaInfo specificJsonSchema.SchemaInfo

            let genericJsonSchema =
                GenericJsonSchema({ SchemaInfo = jsonSchemaInfo; SchemaVersion = version })
                :> ISchema<GenericRecord>
            let specificGenericJsonSchema = genericJsonSchema.GetSpecificSchema(jsonSchemaInfo, version)
            assertSchemaInfo jsonSchemaInfo specificGenericJsonSchema.SchemaInfo

            let protobufSchema = Schema.PROTOBUF<ProtobufSchemaTest>()
            let protobufSchemaInfo = {
                protobufSchema.SchemaInfo with
                    Name = "protobuf-writer"
                    Properties = properties
            }
            let specificProtobufSchema = protobufSchema.GetSpecificSchema(protobufSchemaInfo, version)
            assertSchemaInfo protobufSchemaInfo specificProtobufSchema.SchemaInfo

            let protobufNativeSchema = Schema.PROTOBUF_NATIVE<ProtobufNativeSchemaTest>()
            let protobufNativeSchemaInfo = {
                protobufNativeSchema.SchemaInfo with
                    Name = "protobuf-native-writer"
                    Properties = properties
            }
            let specificProtobufNativeSchema =
                protobufNativeSchema.GetSpecificSchema(protobufNativeSchemaInfo, version)
            assertSchemaInfo protobufNativeSchemaInfo specificProtobufNativeSchema.SchemaInfo

            let genericProtobufNativeSchema =
                GenericProtobufNativeSchema({
                    SchemaInfo = protobufNativeSchemaInfo
                    SchemaVersion = version
                })
                :> ISchema<GenericRecord>
            let specificGenericProtobufNativeSchema =
                genericProtobufNativeSchema.GetSpecificSchema(protobufNativeSchemaInfo, version)
            assertSchemaInfo protobufNativeSchemaInfo specificGenericProtobufNativeSchema.SchemaInfo
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
