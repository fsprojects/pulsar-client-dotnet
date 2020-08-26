namespace Pulsar.Client.Schema

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal KeyValueSchema =
    static member KEY_SCHEMA_NAME = "key.schema.name"
    static member KEY_SCHEMA_TYPE = "key.schema.type"
    static member KEY_SCHEMA_PROPS = "key.schema.properties"
    static member VALUE_SCHEMA_NAME = "value.schema.name"
    static member VALUE_SCHEMA_TYPE = "value.schema.type"
    static member VALUE_SCHEMA_PROPS = "value.schema.properties"
    static member KV_ENCODING_TYPE = "kv.encoding.type"
    
    static member GetKeyValueBytes (keyBytes: byte[], valueBytes: byte[]) =
        let result = Array.create (4 + keyBytes.Length + 4 + valueBytes.Length) 0uy
        use stream = new MemoryStream(result)
        use binaryWriter = new BinaryWriter(stream)
        binaryWriter.Write(int32ToBigEndian keyBytes.Length)
        binaryWriter.Write(keyBytes)
        binaryWriter.Write(int32ToBigEndian valueBytes.Length)
        binaryWriter.Write(valueBytes)
        result        
    static member SeparateKeyAndValueBytes (keyValueBytes: byte[]) =
        use stream = new MemoryStream(keyValueBytes)
        use binaryWriter = new BinaryReader(stream)
        let keyLength = binaryWriter.ReadInt32() |> int32FromBigEndian
        let keyBytes = binaryWriter.ReadBytes(keyLength)
        let valueLength = binaryWriter.ReadInt32() |> int32FromBigEndian
        let valueBytes = binaryWriter.ReadBytes(valueLength)
        (keyBytes, valueBytes)
    static member DecodeKeyValueSchemaInfo(schemaInfo: SchemaInfo) =
        let (keySchemaData, valueSchemaData) = KeyValueSchema.SeparateKeyAndValueBytes schemaInfo.Schema
        let keySchemaInfo = KeyValueSchema.DecodeSubSchemaInfo(schemaInfo, KeyValueSchema.KEY_SCHEMA_NAME,
                                                          KeyValueSchema.KEY_SCHEMA_TYPE, KeyValueSchema.KEY_SCHEMA_PROPS, keySchemaData)
        let valueSchemaInfo = KeyValueSchema.DecodeSubSchemaInfo(schemaInfo, KeyValueSchema.VALUE_SCHEMA_NAME,
                                                          KeyValueSchema.VALUE_SCHEMA_TYPE, KeyValueSchema.VALUE_SCHEMA_PROPS, valueSchemaData)
        (keySchemaInfo, valueSchemaInfo)
    static member private DecodeSubSchemaInfo (parentSchemaInfo: SchemaInfo, schemaNameProperty, schemaTypeProperty, schemaPropsProperty, schemaData) =
        let schemaName =
            match parentSchemaInfo.Properties.TryGetValue schemaNameProperty with
            | true, name -> name
            | _ -> ""
        let schemaType =
            match parentSchemaInfo.Properties.TryGetValue schemaTypeProperty with
            | true, schemaTypeString ->
                match SchemaType.TryParse schemaTypeString with
                | true, schType -> schType
                | _ -> failwith "Invalid schemaType passed"
            | _ ->
                SchemaType.BYTES
        let schemaProps =
            match parentSchemaInfo.Properties.TryGetValue schemaPropsProperty with
            | true, schemaPropsString when (not <| String.IsNullOrEmpty schemaPropsString) ->
                JsonSerializer.Deserialize<IReadOnlyDictionary<string, string>>(schemaPropsString)
            | _ ->
                Map.empty :> IReadOnlyDictionary<string, string>
        {
            Name = schemaName
            Type = schemaType
            Schema = schemaData
            Properties = schemaProps
        }
                
    static member EncodeKeyValueSchemaInfo(schemaName, keySchemaInfo: SchemaInfo, valueSchemaInfo: SchemaInfo, keyValueEncodingType) =
        let kvSchemaData = KeyValueSchema.GetKeyValueBytes(keySchemaInfo.Schema, valueSchemaInfo.Schema)
        let keyProps = KeyValueSchema.EncodeSubSchemaInfo(keySchemaInfo, KeyValueSchema.KEY_SCHEMA_NAME,
                                                          KeyValueSchema.KEY_SCHEMA_TYPE, KeyValueSchema.KEY_SCHEMA_PROPS)
        let valueProps = KeyValueSchema.EncodeSubSchemaInfo(keySchemaInfo, KeyValueSchema.VALUE_SCHEMA_NAME,
                                                          KeyValueSchema.VALUE_SCHEMA_TYPE, KeyValueSchema.VALUE_SCHEMA_PROPS)
        let properties =
            seq {
                yield! keyProps
                yield! valueProps
                yield (KeyValueSchema.KV_ENCODING_TYPE, keyValueEncodingType.ToString())
            } |> readOnlyDict
        {
            Name = schemaName
            Type = SchemaType.KEY_VALUE
            Schema = kvSchemaData
            Properties = properties
        }
    static member private EncodeSubSchemaInfo (schemaInfo: SchemaInfo, schemaNameProperty, schemaTypeProperty, schemaPropsProperty) =
        seq {
            schemaNameProperty, schemaInfo.Name
            schemaTypeProperty, schemaInfo.Type.ToString()
            schemaPropsProperty, JsonSerializer.Serialize(schemaInfo.Properties)
        }


type internal KeyValueSchema<'K,'V>(keySchema: ISchema<'K>, valueSchema: ISchema<'V>, kvType: KeyValueEncodingType) =
    inherit ISchema<KeyValuePair<'K,'V>>()
    member this.KeyValueEncodingType = kvType
    member this.KeySchema = keySchema
    member this.ValueSchema = valueSchema
    member this.Decode (keyBytes, valueBytes) =
        let k = keySchema.Decode(keyBytes)
        let v = valueSchema.Decode(valueBytes)
        KeyValuePair(k, v)
    override this.SchemaInfo =
        KeyValueSchema.EncodeKeyValueSchemaInfo("KeyValue", keySchema.SchemaInfo, valueSchema.SchemaInfo, kvType)
    override this.SupportSchemaVersioning =
        keySchema.SupportSchemaVersioning && valueSchema.SupportSchemaVersioning
    override this.Encode (KeyValue(key, value)) =
        match kvType with
        | KeyValueEncodingType.INLINE ->
            let keyBytes = keySchema.Encode(key)
            let valueBytes = valueSchema.Encode(value)
            KeyValueSchema.GetKeyValueBytes(keyBytes, valueBytes)
        | KeyValueEncodingType.SEPARATED ->
            valueSchema.Encode(value)
        | _ ->
            failwith "Unsupported KeyValueEncodingType"
    override this.Decode bytes =
        match kvType with
        | KeyValueEncodingType.INLINE ->
            let (keyBytes, valueBytes) = KeyValueSchema.SeparateKeyAndValueBytes(bytes)
            let key = keySchema.Decode(keyBytes)
            let value = valueSchema.Decode(valueBytes)
            KeyValuePair(key, value)
        | KeyValueEncodingType.SEPARATED ->
            raise <| SchemaSerializationException "This method cannot be used under this SEPARATED encoding type"
        | _ ->
            failwith "Unsupported KeyValueEncodingType"
        

type internal IKeyValueProcessor =
    abstract member EncodeKeyValue: obj -> struct(string * byte[])
    abstract member DecodeKeyValue: string * byte[] -> obj
    abstract member EncodingType: KeyValueEncodingType

type internal KeyValueProcessor<'K,'V>(schema: KeyValueSchema<'K,'V>) =
    
    interface IKeyValueProcessor with
        member this.EncodeKeyValue value =
            if isNull value then
                raise <| SchemaSerializationException "Need Non-Null key"
            let (KeyValue(k, v)) = value :?> KeyValuePair<'K,'V>
            let keyBytes = schema.KeySchema.Encode(k)
            let content = schema.ValueSchema.Encode(v)
            let strKey = if isNull keyBytes then null else keyBytes |> Convert.ToBase64String
            struct(strKey, content)
        member this.DecodeKeyValue(strKey: string, content) =
            let keyBytes = strKey |> Convert.FromBase64String
            schema.Decode(keyBytes, content) |> box                
        member this.EncodingType =
            schema.KeyValueEncodingType
                
type internal KeyValueProcessor =
    static member GetInstance (schema: ISchema<'T>) =
        if schema.SchemaInfo.Type = SchemaType.KEY_VALUE && schema.GetType().Name <> "AutoProduceBytesSchema" then
            let kvType = typeof<'T>
            let kvpTypeTemplate = typedefof<KeyValueProcessor<_,_>>
            let kvpType = kvpTypeTemplate.MakeGenericType(kvType.GetGenericArguments())
            let obj = Activator.CreateInstance(kvpType, schema)
            let kvp = (obj :?> IKeyValueProcessor)
            if kvp.EncodingType = KeyValueEncodingType.SEPARATED then
                Some kvp
            else
                None
        else
            None
