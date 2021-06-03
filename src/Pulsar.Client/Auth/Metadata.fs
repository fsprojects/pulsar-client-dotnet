module Pulsar.Client.Auth.Metadata

open System.Text.Json.Serialization
open System

type Metadata =
    
    [<JsonPropertyName("issuer")>]
    member x.issuer:string =  String.Empty
    
    [<JsonPropertyName("token_endpoint")>]
    member x.tokenEndpoint:string =  String.Empty
    
    [<JsonPropertyName("userinfo_endpoint")>]
    member x.userInfoEndpoint:string =  String.Empty
    
    [<JsonPropertyName("revocation_endpoint")>]
    member x.revocationEndpoint:string =  String.Empty
    
    [<JsonPropertyName("jwks_uri")>]
    member x.jwksUri:string =  String.Empty
    
    [<JsonPropertyName("device_authorization_endpoint")>]
    member x.deviceAuthorizationEndpoint:string =  String.Empty