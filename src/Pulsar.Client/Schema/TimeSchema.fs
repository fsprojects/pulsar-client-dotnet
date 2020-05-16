namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open System
open Pulsar.Client.Common

type internal TimeSchema() =
    inherit ISchema<TimeSpan>()
    override this.SchemaInfo = { Name = "Time"; Type = SchemaType.TIME; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        value.TotalMilliseconds
        |> int64
        |> int64ToBigEndian
        |> BitConverter.GetBytes
    override this.Decode bytes =
        BitConverter.ToInt64(bytes, 0)        
        |> int64FromBigEndian
        |> float
        |> TimeSpan.FromMilliseconds