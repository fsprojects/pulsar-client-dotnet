namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type BytesSchema() =
    inherit ISchema<byte[]>()
    override this.SchemaInfo = { Name = "Bytes"; Type = SchemaType.BYTES; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        if isNull value then
            raise <| SchemaSerializationException "Need Non-Null content value"
        value
    override this.Decode bytes = bytes