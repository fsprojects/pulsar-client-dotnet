namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal FloatSchema() =
    inherit ISchema<single>()
    override this.SchemaInfo = { Name = "Float"; Type = SchemaType.FLOAT; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> BitConverter.GetBytes
        |> Array.rev
    override this.Decode bytes =
        this.Validate bytes
        BitConverter.ToSingle(bytes |> Array.rev, 0)
    override this.Validate bytes =
        if bytes.Length <> 4 then
            raise <| SchemaSerializationException "Size of data received by FloatSchema is not 4"

