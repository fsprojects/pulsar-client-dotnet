namespace Pulsar.Client.Schema

open ProtoBuf
open System.IO
open Pulsar.Client.Common
open System.Text
open Pulsar.Client.Api
open AvroSchemaGenerator

type internal ProtobufSchema<'T>() =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    let stringSchema = typeof<'T>.GetSchema()
    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.PROTOBUF
        Schema = stringSchema |> Encoding.UTF8.GetBytes
        Properties = Map.empty
    }
    override this.Encode value =
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        use stream = MemoryStreamManager.GetStream()
        Serializer.Serialize(stream, value)
        stream.ToArray()
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        Serializer.Deserialize(stream)

