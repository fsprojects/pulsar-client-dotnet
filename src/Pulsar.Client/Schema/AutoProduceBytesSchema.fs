namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal AutoProduceBytesSchemaStub() =
    inherit ISchema<byte[]>()
    override this.SchemaInfo = { Name = "AutoProduce"; Type = SchemaType.AUTO_PUBLISH; Schema = [||]; Properties = Map.empty }
    override this.Encode _ = raise <| SchemaSerializationException "AutoProduceBytesSchema is just stub!"
    override this.Decode _ = raise <| SchemaSerializationException "AutoProduceBytesSchema is just stub!"
    
type internal AutoProduceBytesSchema(name, schemaType, schemaData, validate) =
    inherit ISchema<byte[]>()
    override this.SchemaInfo = { Name = name; Type = schemaType; Schema = schemaData; Properties = Map.empty }
    override this.Validate bytes =
        validate bytes
    override this.Encode value =
        validate value
        value
    override this.Decode _ =
        raise <| SchemaSerializationException "AutoProduceBytesSchema is only used for encoding!"


