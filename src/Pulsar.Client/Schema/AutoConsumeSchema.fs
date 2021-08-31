namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal AutoConsumeSchemaStub() =
    inherit ISchema<GenericRecord>()
    override this.SchemaInfo = { Name = "AutoConsume"; Type = SchemaType.AUTO_CONSUME; Schema = [||]; Properties = Map.empty }
    override this.Encode _ = raise <| SchemaSerializationException "AutoConsumeSchema is just stub!"
    override this.Decode _ = raise <| SchemaSerializationException "AutoConsumeSchema is just stub!"
    override this.SupportSchemaVersioning = true