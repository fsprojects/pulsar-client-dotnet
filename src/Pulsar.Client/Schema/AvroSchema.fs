namespace Pulsar.Client.Schema

open System.Text
open Avro
open Avro.Generic
open Avro.IO
open Avro.Reflect
open System.IO
open Avro.Specific
open Pulsar.Client.Api
open AvroSchemaGenerator
open Pulsar.Client.Common

type internal AvroSchema<'T> private (schema: Schema, avroReader: DatumReader<'T>, avroWriter: DatumWriter<'T> option,
                                     ?schemaInfo: SchemaInfo) =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    let defaultValue = Unchecked.defaultof<'T>
    let schemaInfo =
        schemaInfo
        |> Option.defaultWith (fun () -> {
            Name = ""
            Type = SchemaType.AVRO
            Schema = schema.ToString() |> Encoding.UTF8.GetBytes
            Properties = Map.empty
        })

    new () =
         let tpe = typeof<'T>
         if typeof<ISpecificRecord>.IsAssignableFrom(tpe) then
            let avroSchema = tpe.GetField("_SCHEMA").GetValue(null) :?> Schema
            let avroWriter = SpecificDatumWriter<'T>(avroSchema) :> DatumWriter<'T>
            let avroReader = SpecificDatumReader<'T>(avroSchema, avroSchema)
            AvroSchema(avroSchema, avroReader, Some avroWriter)
         else
            let schemaString = tpe.GetSchema()
            AvroSchema(schemaString)

    new (schemaString) =
        let avroSchema = Schema.Parse(schemaString)
        let avroWriter = ReflectWriter<'T>(avroSchema) :> DatumWriter<'T>
        let avroReader = ReflectReader<'T>(avroSchema, avroSchema)
        AvroSchema(avroSchema, avroReader, Some avroWriter)

    override this.SchemaInfo = schemaInfo
    override this.SupportSchemaVersioning = true
    override this.Encode value =
        match avroWriter with
        | Some avroWriter ->
            if parameterIsClass && (isNull <| box value) then
                raise <| SchemaSerializationException "Need Non-Null content value"
            use stream = MemoryStreamManager.GetStream("AvroEncode")
            avroWriter.Write(value, BinaryEncoder(stream))
            stream.ToArray()
        | None ->
            raise <| SchemaSerializationException "Schema resolved at a specific version is for consuming only!"
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        avroReader.Read(defaultValue, BinaryDecoder(stream))
    override this.GetSpecificSchema (writtenSchemaInfo, _) =
        // The returned schema decodes data written with writtenSchemaInfo and reports it as its own
        // SchemaInfo, so that the schema identifies the data's writer (as Java's atSchemaVersion does).
        // It carries no writer, so it can only be used for consuming and for republishing the original
        // payload through Schema.AUTO_PRODUCE, never for encoding new values.
        let writtenSchema = Schema.Parse(writtenSchemaInfo.Schema |> Encoding.UTF8.GetString)
        if avroReader :? SpecificDatumReader<'T> then
            AvroSchema(schema, SpecificDatumReader(writtenSchema, schema), None, writtenSchemaInfo) :> ISchema<'T>
        else
            if writtenSchema.Fullname <> schema.Fullname then
                // Avro doesnt figure that the written classname might be different from the reader classname
                // Seems like it might be a bug in ReflectReader, but this works around that
                let cache = ClassCache()
                cache.LoadClassCache(typeof<'T>, writtenSchema)
                AvroSchema(schema, ReflectReader<'T>(writtenSchema, schema, cache), None, writtenSchemaInfo) :> ISchema<'T>
            else
                AvroSchema(schema, ReflectReader<'T>(writtenSchema, schema), None, writtenSchemaInfo) :> ISchema<'T>

type internal GenericAvroSchema(schemaInfo: SchemaInfo, schemaVersion: SchemaVersion option) =
    inherit ISchema<GenericRecord>()
    let stringSchema = schemaInfo.Schema |> Encoding.UTF8.GetString
    let avroSchema = Schema.Parse(stringSchema) :?> RecordSchema
    let avroReader = GenericDatumReader<Avro.Generic.GenericRecord>(avroSchema, avroSchema)
    let schemaFields = avroSchema.Fields

    new(topicSchema) =
        GenericAvroSchema(topicSchema.SchemaInfo, topicSchema.SchemaVersion)

    override this.SchemaInfo = schemaInfo
    override this.SupportSchemaVersioning = true
    override this.Encode _ = raise <| SchemaSerializationException "GenericAvroSchema is for consuming only!"
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        let record = avroReader.Read(null, BinaryDecoder(stream))
        let fields =
            schemaFields
            |> Seq.map (fun sf -> { Name = sf.Name; Value = record.[sf.Name]; Index = sf.Pos })
            |> Seq.toArray
        let schemaVersionBytes =
            schemaVersion
            |> Option.map _.Bytes
            |> Option.toObj
        GenericRecord(schemaVersionBytes, fields)

    override this.GetSpecificSchema (schemaInfo, schemaVersion) =
        GenericAvroSchema(schemaInfo,schemaVersion) :> ISchema<_>
