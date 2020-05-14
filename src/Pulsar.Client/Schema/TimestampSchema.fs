namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal TimestampSchema() =
    inherit ISchema<DateTimeOffset>()
    override this.SchemaInfo = { Name = "Timestamp"; Type = SchemaType.TIMESTAMP; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value.ToUnixTimeMilliseconds()
        |> int64ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        BitConverter.ToInt64(bytes, 0)        
        |> int64FromBigEndian
        |> DateTimeOffset.FromUnixTimeMilliseconds