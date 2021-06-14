module internal Pulsar.Client.Auth.DefaultImplementation

open System
open Pulsar.Client.Auth
open Pulsar.Client.Auth.OAuth2

let newAuthenticationToken (token: string) =
    new AuthenticationToken (token)

let newAuthenticationTls (certFilePath: string) =
    new AuthenticationTls (certFilePath)
    
let newAuthenticationOauth2 (issuerUrl : Uri, credentialsJson: Uri, audience: string) =
    new AuthenticationOauth2 (issuerUrl, credentialsJson, audience)