namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal BooleanSchema() =
    inherit ISchema<bool>()
    override this.SchemaInfo = { Name = "Boolean"; Type = SchemaType.BOOLEAN; Schema = [||]; Properties = Map.empty }
    override this.Encode value = [| if value then 1uy else 0uy |]
    override this.Decode bytes =
        this.Validate bytes
        bytes.[0] <> 0uy
    override this.Validate bytes =
        if bytes.Length <> 1 then
            raise <| SchemaSerializationException "Size of data received by BooleanSchema is not 1"

