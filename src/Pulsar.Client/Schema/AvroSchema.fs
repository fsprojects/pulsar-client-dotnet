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

type internal AvroSchema<'T> private (schema: Schema, avroReader: DatumReader<'T>, avroWriter: DatumWriter<'T>) =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    let defaultValue = Unchecked.defaultof<'T>
    
    new () =
         let tpe = typeof<'T>
         if typeof<ISpecificRecord>.IsAssignableFrom(tpe) then
            let avroSchema = tpe.GetField("_SCHEMA").GetValue(null) :?> Schema
            let avroWriter = SpecificDatumWriter<'T>(avroSchema)
            let avroReader = SpecificDatumReader<'T>(avroSchema, avroSchema)
            AvroSchema(avroSchema, avroReader, avroWriter)
         else
            let schemaString = tpe.GetSchema()
            AvroSchema(schemaString)

    new (schemaString) =
        let avroSchema = Schema.Parse(schemaString)
        let avroWriter = ReflectWriter<'T>(avroSchema)
        let avroReader = ReflectReader<'T>(avroSchema, avroSchema)
        AvroSchema(avroSchema, avroReader, avroWriter)
        
    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.AVRO
        Schema = schema.ToString() |> Encoding.UTF8.GetBytes
        Properties = Map.empty
    }
    override this.SupportSchemaVersioning = true
    override this.Encode value =
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        use stream = MemoryStreamManager.GetStream()
        avroWriter.Write(value, BinaryEncoder(stream))
        stream.ToArray()
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        avroReader.Read(defaultValue, BinaryDecoder(stream))
    override this.GetSpecificSchema (schemaInfo, _) =
        let writtenSchema = Schema.Parse(schemaInfo.Schema |> Encoding.UTF8.GetString)
        if avroReader :? SpecificDatumReader<'T> then
            AvroSchema(schema, SpecificDatumReader(writtenSchema, schema), avroWriter) :> ISchema<'T>
        else
            if writtenSchema.Fullname <> schema.Fullname then
                // Avro doesnt figure that the written classname might be different from the reader classname
                // Seems like it might be a bug in ReflectReader, but this works around that
                let cache = ClassCache()
                cache.LoadClassCache(typeof<'T>, writtenSchema)
                AvroSchema(schema, ReflectReader<'T>(writtenSchema, schema, cache), avroWriter) :> ISchema<'T>
            else
                AvroSchema(schema, ReflectReader<'T>(writtenSchema, schema), avroWriter) :> ISchema<'T>
        
type internal GenericAvroSchema(schemaInfo: SchemaInfo, schemaVersion: SchemaVersion option) =
    inherit ISchema<GenericRecord>()
    let stringSchema = schemaInfo.Schema |> Encoding.UTF8.GetString
    let avroSchema = Schema.Parse(stringSchema) :?> RecordSchema
    let avroReader = GenericDatumReader<Avro.Generic.GenericRecord>(avroSchema, avroSchema)
    let schemaFields = avroSchema.Fields
    
    new(topicSchema) = 
        GenericAvroSchema(topicSchema.SchemaInfo, topicSchema.SchemaVersion)

    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.AVRO
        Schema = schemaInfo.Schema
        Properties = Map.empty
    }
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
            |> Option.map (fun sv -> sv.Bytes)
            |> Option.toObj
        GenericRecord(schemaVersionBytes, fields)
        
    override this.GetSpecificSchema (schemaInfo, schemaVersion) =
        GenericAvroSchema(schemaInfo,schemaVersion) :> ISchema<_>