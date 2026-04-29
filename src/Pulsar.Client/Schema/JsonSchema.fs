namespace Pulsar.Client.Schema

open System
open System.Collections.Generic
open System.Dynamic
open System.Text
open System.Text.Json
open System.Text.Json.Serialization
open Avro
open Pulsar.Client.Api
open AvroSchemaGenerator
open Pulsar.Client.Common

type internal DateTimeTimestampMillisConverter() =
    inherit JsonConverter<DateTime>()
    let normalizeToUtc (value: DateTime) =
        if value.Kind = DateTimeKind.Utc then
            value
        else
            DateTime.SpecifyKind(value, DateTimeKind.Utc)

    override this.Read(reader: byref<Utf8JsonReader>, _, _) =
        match reader.TokenType with
        | JsonTokenType.Number ->
            reader.GetInt64()
            |> DateTimeOffset.FromUnixTimeMilliseconds
            |> _.UtcDateTime
        | JsonTokenType.String ->
            reader.GetDateTime()
        | _ ->
            raise <| JsonException $"Unexpected token parsing DateTime. Expected Number or String, got {reader.TokenType}."

    override this.Write(writer: Utf8JsonWriter, value: DateTime, _) =
        value
        |> normalizeToUtc
        |> DateTimeOffset
        |> _.ToUnixTimeMilliseconds()
        |> writer.WriteNumberValue

type internal JsonSchema<'T> () =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    let options = JsonSerializerOptions(IgnoreNullValues = true)
    do options.Converters.Add(DateTimeTimestampMillisConverter())
    let stringSchema = typeof<'T>.GetSchema()
    override this.SchemaInfo = { Name = ""; Type = SchemaType.JSON; Schema = stringSchema |> Encoding.UTF8.GetBytes; Properties = Map.empty }
    override this.Encode value =
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        JsonSerializer.SerializeToUtf8Bytes(value, options)
    override this.Decode bytes =
        JsonSerializer.Deserialize<'T>(ReadOnlySpan bytes, options)

type internal GenericJsonSchema (topicSchema: TopicSchema) =
    inherit ISchema<GenericRecord>()
    let dynamicSerializerOptions = JsonSerializerOptions(IgnoreNullValues = true)
    do dynamicSerializerOptions.Converters.Add <| DynamicJsonConverter()
    let stringSchema = topicSchema.SchemaInfo.Schema |> Encoding.UTF8.GetString
    let avroSchema = Schema.Parse(stringSchema) :?> RecordSchema
    let schemaFields = avroSchema.Fields
    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.JSON
        Schema = topicSchema.SchemaInfo.Schema
        Properties = Map.empty
    }
    override this.Encode _ = raise <| SchemaSerializationException "GenericJsonSchema is for consuming only!"
    override this.Decode bytes =
        let doc = JsonSerializer.Deserialize<IDictionary<string, obj>>(ReadOnlySpan bytes, dynamicSerializerOptions)
        let fields =
            schemaFields
            |> Seq.map (fun sf -> { Name = sf.Name; Value = doc.[sf.Name]; Index = sf.Pos })
            |> Seq.toArray
        let scemaVersionBytes =
            topicSchema.SchemaVersion
            |> Option.map _.Bytes
            |> Option.toObj
        GenericRecord(scemaVersionBytes, fields)