module internal Pulsar.Client.Auth.DefaultImplementation

open System

let newAuthenticationToken (token: string) =
    AuthenticationToken token

let newAuthenticationTls (certFilePath: string) =
    AuthenticationTls certFilePath
    
let newAuthenticationOauth2 (issuerUrl : Uri, credentialsJson: string, audience: Uri) =
    AuthenticationOauth2 (issuerUrl,credentialsJson,audience)