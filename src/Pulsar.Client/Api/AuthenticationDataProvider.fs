namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System.Text

type AuthenticationDataProvider() =

    abstract member HasDataFromCommand: unit -> bool
    default this.HasDataFromCommand() =
        false

    abstract member GetCommandData: unit -> string
    default this.GetCommandData() =
        ""

    abstract member Authenticate: AuthData -> AuthData
    default this.Authenticate(authData) =
        let bytes =
            if this.HasDataFromCommand() then this.GetCommandData() else ""
            |> Encoding.UTF8.GetBytes
        { Bytes = bytes }
