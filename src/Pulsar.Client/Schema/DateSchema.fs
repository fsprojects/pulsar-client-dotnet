namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type DateSchema() =
    inherit ISchema<DateTime>()
    override this.SchemaInfo = { Name = "Date"; Type = SchemaType.DATE; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value
        |> convertToMsTimestamp
        |> int64ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        BitConverter.ToInt64(bytes, 0)        
        |> int64FromBigEndian
        |> convertToDateTime