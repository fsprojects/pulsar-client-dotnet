module Pulsar.Client.Api.AuthenticationFactory

open System
open Pulsar.Client.Auth
open Pulsar.Client.Auth.Oauth2

let token (token: string) : Authentication =
    DefaultImplementation.newAuthenticationToken token :> Authentication

let tls (certFilePath: string) : Authentication =
    DefaultImplementation.newAuthenticationTls certFilePath :> Authentication
    
let oauth2 (issuerUrl : Uri, credentialsJson: Uri, audience: Uri) =
    DefaultImplementation.newAuthenticationOauth2 (issuerUrl, credentialsJson, audience)
    :> Authentication