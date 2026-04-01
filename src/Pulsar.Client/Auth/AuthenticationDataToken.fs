namespace Pulsar.Client.Auth    

open Pulsar.Client.Api
open System.Collections.Generic

type internal AuthenticationDataToken (supplier: unit -> string) =
    inherit AuthenticationDataProvider()

    override this.HasDataFromCommand() =
        true

    override this.GetCommandData() =
        supplier()

    override this.HasDataForHttp() =
        true

    override this.GetHttpHeaders() =
        seq {
            yield KeyValuePair("Authorization", $"Bearer {supplier()}")
        }
