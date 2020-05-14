namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal StringSchema (charset: System.Text.Encoding) =
    inherit ISchema<string>()
    override this.SchemaInfo = { Name = "String"; Type = SchemaType.STRING; Schema = [||]; Properties = Map.empty }
    override this.Encode value =
        if isNull value then
            raise <| SchemaSerializationException "Need Non-Null content value"
        charset.GetBytes(value)
    override this.Decode bytes =
        charset.GetString(bytes)

