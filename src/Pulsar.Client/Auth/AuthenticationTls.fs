namespace Pulsar.Client.Auth

open System.IO
open Pulsar.Client.Api

type internal AuthenticationTls (certFilePath: string) =
    inherit Authentication()

    override this.GetAuthMethodName() =
        "tls"
    override this.GetAuthData() =
        AuthenticationDataTls(FileInfo(certFilePath)) :> AuthenticationDataProvider