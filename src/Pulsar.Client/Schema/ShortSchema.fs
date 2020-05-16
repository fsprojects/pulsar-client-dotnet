namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal ShortSchema() =
    inherit ISchema<int16>()
    override this.SchemaInfo = { Name = "INT16"; Type = SchemaType.INT16; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> int16ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        this.Validate bytes
        BitConverter.ToInt16(bytes, 0)
        |> int16FromBigEndian
    override this.Validate bytes =
        if bytes.Length <> 2 then
            raise <| SchemaSerializationException "Size of data received by ShortSchema is not 2"

