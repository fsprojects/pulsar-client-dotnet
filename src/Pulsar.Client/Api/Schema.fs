namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open Avro.Generic
open FSharp.UMX
open Google.Protobuf
open Pulsar.Client.Common
open Pulsar.Client.Schema
open System.Text

[<AbstractClass>]
type Schema =
    
    static member BYTES() =
        BytesSchema() :> ISchema<byte[]>
    static member BOOL() =
        BooleanSchema() :> ISchema<bool>
    static member INT8() =
        ByteSchema() :> ISchema<byte>
    static member INT16() =
        ShortSchema() :> ISchema<int16>
    static member INT32() =
        IntSchema() :> ISchema<int>
    static member INT64() =
        LongSchema() :> ISchema<int64>
    static member FLOAT() =
        FloatSchema() :> ISchema<single>
    static member DOUBLE() =
        DoubleSchema() :> ISchema<float>
    static member DATE() =
        DateSchema() :> ISchema<DateTime>
    static member TIME() =
        TimeSchema() :> ISchema<TimeSpan>
    static member TIMESTAMP() =
        TimestampSchema() :> ISchema<DateTimeOffset>
    static member STRING ([<Optional; DefaultParameterValue(null:Encoding)>]charset: Encoding) =
        let charset = if isNull charset then Encoding.UTF8 else charset
        StringSchema(charset) :> ISchema<string>
    static member JSON<'T> () =
        JsonSchema<'T>() :> ISchema<'T>
    static member PROTOBUF<'T> () =
        ProtobufSchema<'T>() :> ISchema<'T>
    static member AVRO<'T> () =
        AvroSchema<'T>() :> ISchema<'T>        
    static member PROTOBUF_NATIVE<'T > () =
        ProtoBufNativeSchema<'T>() :> ISchema<'T>        
        
    static member KEY_VALUE<'K,'V> (schemaType: SchemaType) =
        match schemaType with
        | SchemaType.JSON -> KeyValueSchema<'K, 'V>(JsonSchema<'K>(), JsonSchema<'V>(), KeyValueEncodingType.INLINE)
        | _ -> KeyValueSchema<'K, 'V>(AvroSchema<'K>(), AvroSchema<'V>(), KeyValueEncodingType.INLINE)
        :> ISchema<KeyValuePair<'K,'V>>
    static member KEY_VALUE<'K,'V>(keySchema: ISchema<'K>, valueSchema: ISchema<'V>, kvType: KeyValueEncodingType) =
        KeyValueSchema(keySchema, valueSchema, kvType) :> ISchema<KeyValuePair<'K,'V>>
    static member AUTO_PRODUCE() =
        AutoProduceBytesSchemaStub() :> ISchema<byte[]>
    static member AUTO_CONSUME() =
        AutoConsumeSchemaStub() :> ISchema<Pulsar.Client.Api.GenericRecord>
    static member internal GetValidateFunction (topicSchema: TopicSchema) =
        let schema = topicSchema.SchemaInfo
        match schema.Type with
        | SchemaType.NONE | SchemaType.BYTES -> ignore
        | SchemaType.BOOLEAN -> Schema.BOOL().Validate
        | SchemaType.INT8 -> Schema.INT8().Validate
        | SchemaType.INT16 -> Schema.INT16().Validate
        | SchemaType.INT32 -> Schema.INT32().Validate
        | SchemaType.INT64 -> Schema.INT64().Validate
        | SchemaType.DATE -> Schema.DATE().Validate
        | SchemaType.TIME -> Schema.TIME().Validate
        | SchemaType.TIMESTAMP -> Schema.TIMESTAMP().Validate
        | SchemaType.STRING -> Schema.STRING().Validate
        | SchemaType.JSON -> GenericJsonSchema(topicSchema).Validate
        | SchemaType.AVRO -> GenericAvroSchema(topicSchema).Validate
        | SchemaType.KEY_VALUE ->
            let (keySchemaData, valueSchemaData) = KeyValueSchema.DecodeKeyValueSchemaInfo(topicSchema.SchemaInfo)
            let keyValidation = Schema.GetValidateFunction({topicSchema with SchemaInfo = keySchemaData})
            let dataValidation = Schema.GetValidateFunction({topicSchema with SchemaInfo = valueSchemaData})
            fun bytes ->
                keyValidation bytes
                dataValidation bytes
        | _ -> raise <| ArgumentException $"Retrieve schema instance from schema info for type {schema.Type} is not supported yet"        
    static member internal GetAutoConsumeSchema (topicSchema: TopicSchema) =
        let schema = topicSchema.SchemaInfo
        match schema.Type with
        | SchemaType.PROTOBUF_NATIVE ->
            GenericProtobufNativeSchema(topicSchema):> ISchema<_>
        | SchemaType.JSON ->
            GenericJsonSchema(topicSchema) :> ISchema<_>
        | SchemaType.AVRO ->
            GenericAvroSchema(topicSchema) :> ISchema<_>        
        | _ -> raise <| ArgumentException $"Auto consumer for type {schema.Type} is not supported yet"