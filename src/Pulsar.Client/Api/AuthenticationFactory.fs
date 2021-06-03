module Pulsar.Client.Api.AuthenticationFactory

open System
open Pulsar.Client.Auth

let token (token: string) : Authentication =
    DefaultImplementation.newAuthenticationToken token :> Authentication

let tls (certFilePath: string) : Authentication =
    DefaultImplementation.newAuthenticationTls certFilePath :> Authentication
    
let oauth2 (issuerUrl : Uri, credentialsJson: string, audience: Uri) =
    DefaultImplementation.newAuthenticationOauth2 (issuerUrl, credentialsJson, audience)
    :> Authentication