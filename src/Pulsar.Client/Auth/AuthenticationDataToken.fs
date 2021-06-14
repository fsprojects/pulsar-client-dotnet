namespace Pulsar.Client.Auth    

open Pulsar.Client.Api

type internal AuthenticationDataToken (supplier: unit -> string) =
    inherit AuthenticationDataProvider()

    override this.HasDataFromCommand() =
        true

    override this.GetCommandData() =
        supplier()