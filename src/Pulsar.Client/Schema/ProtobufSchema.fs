namespace Pulsar.Client.Schema

open ProtoBuf
open System.IO
open Pulsar.Client.Common
open System.Text
open Pulsar.Client.Api
open AvroSchemaGenerator

type internal ProtobufSchema<'T> private (schemaInfo: SchemaInfo option) =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    let stringSchema = typeof<'T>.GetSchema()

    new() = ProtobufSchema(None)

    override this.SchemaInfo =
        schemaInfo
        |> Option.defaultWith (fun () -> {
            Name = ""
            Type = SchemaType.PROTOBUF
            Schema = stringSchema |> Encoding.UTF8.GetBytes
            Properties = Map.empty
        })
    override this.SupportSchemaVersioning = true
    override this.Encode value =
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        use stream = MemoryStreamManager.GetStream()
        Serializer.Serialize(stream, value)
        stream.ToArray()
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        Serializer.Deserialize(stream)
    override this.GetSpecificSchema (schemaInfo, _) =
        ProtobufSchema<'T>(Some schemaInfo) :> ISchema<_>
