namespace Pulsar.Client.Auth

open Pulsar.Client.Api

type internal AuthenticationToken (supplier: unit -> string) =
    inherit Authentication()

    new(token: string) =
        new AuthenticationToken (fun () -> token)

    override this.GetAuthMethodName() =
        "token"
    override this.GetAuthData() =
        AuthenticationDataToken(supplier) :> AuthenticationDataProvider
