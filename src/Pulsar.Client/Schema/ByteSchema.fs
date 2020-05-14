namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal ByteSchema() =
    inherit ISchema<byte>()
    override this.SchemaInfo = { Name = "INT8"; Type = SchemaType.INT8; Schema = [||]; Properties = Map.empty }
    override this.Encode value = [| value |]
    override this.Decode bytes =
        this.Validate bytes
        bytes.[0]
    override this.Validate bytes =
        if bytes.Length <> 1 then
            raise <| SchemaSerializationException "Size of data received by ByteSchema is not 1"

