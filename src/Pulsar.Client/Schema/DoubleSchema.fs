namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal DoubleSchema() =
    inherit ISchema<float>()
    override this.SchemaInfo = { Name = "Double"; Type = SchemaType.DOUBLE; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> BitConverter.GetBytes
        |> Array.rev
    override this.Decode bytes =
        this.Validate bytes
        BitConverter.ToDouble(bytes |> Array.rev, 0)
    override this.Validate bytes =
        if bytes.Length <> 8 then
            raise <| SchemaSerializationException "Size of data received by DoubleSchema is not 8"

