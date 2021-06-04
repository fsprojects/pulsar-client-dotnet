module Pulsar.Client.Auth.Metadata

open System.Text.Json.Serialization


type Metadata=
    {
        [<JsonPropertyName("issuer")>]
        issuer:string
        [<JsonPropertyName("token_endpoint")>]
        tokenEndpoint:string
        [<JsonPropertyName("userinfo_endpoint")>]
        userInfoEndpoint:string        
        [<JsonPropertyName("revocation_endpoint")>]
        revocationEndpoint:string         
        [<JsonPropertyName("jwks_uri")>]
        jwksUri:string        
        [<JsonPropertyName("device_authorization_endpoint")>]
        deviceAuthorizationEndpoint:string 
    }