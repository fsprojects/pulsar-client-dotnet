namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal IntSchema() =
    inherit ISchema<int>()
    override this.SchemaInfo = { Name = "INT32"; Type = SchemaType.INT32; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> int32ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        this.Validate bytes
        BitConverter.ToInt32(bytes, 0)
        |> int32FromBigEndian
    override this.Validate bytes =
        if bytes.Length <> 4 then
            raise <| SchemaSerializationException "Size of data received by IntSchema is not 4"

