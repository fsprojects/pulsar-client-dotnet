namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal BytesSchema() =
    inherit ISchema<byte[]>()
    override this.SchemaInfo = { Name = "Bytes"; Type = SchemaType.BYTES; Schema = [||]; Properties = Map.empty }
    override this.Encode value = value
    override this.Decode bytes = bytes