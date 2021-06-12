module Pulsar.Client.Api.AuthenticationFactoryOAuth2

open System
open Pulsar.Client.Auth

let clientCredentials (issuerUrl : Uri, credentialsJson: Uri, audience: Uri) =
    DefaultImplementation.newAuthenticationOauth2 (issuerUrl, credentialsJson, audience)
    :> Authentication