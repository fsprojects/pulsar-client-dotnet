namespace Pulsar.Client.Api

open Pulsar.Client.Common
open pulsar.proto

[<AbstractClass>]
type ISchema<'T>() =
    abstract member SchemaInfo: SchemaInfo
    abstract member Encode: 'T -> byte[]
    abstract member Decode: byte[] -> 'T
    abstract member GetSpecificSchema: SchemaInfo * SchemaVersion option -> ISchema<'T>
    default this.GetSpecificSchema (_, _) =
        this
    abstract member SupportSchemaVersioning: bool
    default this.SupportSchemaVersioning = false
    abstract member Validate: byte[] -> unit
    default this.Validate bytes =
        this.Decode(bytes) |> ignore

type GenericRecord (schemaVersion: byte[], fields: Field[]) =
    member this.SchemaVersion = schemaVersion
    member this.Fields = fields
    member this.GetField(name) =
        (fields |> Array.find (fun el -> el.Name = name)).Value
    member this.TryGetField(name) =
        fields
        |> Array.tryFind (fun field -> field.Name = name)
        |> Option.map(fun field -> field.Value)
        |> Option.toObj
and Field =
    {
        Name: string
        Value: obj
        Index: int
    }

[<AutoOpen>]
module SchemaUtils =
    let getProtoSchemaType (schemaType: SchemaType) =
        let schemaNumber = int schemaType
        if schemaNumber >= 0 then
            enum schemaNumber
        else
            Schema.Type.None

    let getDomainSchemaType (protoSchemaType: Schema.Type) =
        let schemaNumber = int protoSchemaType
        enum<SchemaType> schemaNumber
    
    let getProtoSchema (schemaInfo: SchemaInfo) =
        let schema =
            Schema (
                Name = schemaInfo.Name,
                ``type`` = getProtoSchemaType schemaInfo.Type,
                SchemaData = schemaInfo.Schema
            )
        schemaInfo.Properties
        |> Seq.iter (fun prop -> schema.Properties.Add(KeyValue(Key = prop.Key, Value = prop.Value)))
        schema

    let getSchemaInfo (schema: Schema) =
        {
            Name = schema.Name
            Type = getDomainSchemaType schema.``type``
            Schema = schema.SchemaData
            Properties =
                schema.Properties
                |> Seq.map (fun kv -> kv.Key, kv.Value)
                |> readOnlyDict
        }
    