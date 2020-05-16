namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal LongSchema() =
    inherit ISchema<int64>()
    override this.SchemaInfo = { Name = "INT64"; Type = SchemaType.INT64; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> int64ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        this.Validate bytes
        BitConverter.ToInt64(bytes, 0)
        |> int64FromBigEndian
    override this.Validate bytes =
        if bytes.Length <> 8 then
            raise <| SchemaSerializationException "Size of data received by LongSchema is not 8"

