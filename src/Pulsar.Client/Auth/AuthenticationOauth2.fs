module Pulsar.Client.Auth.Oauth2

open System
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Auth.Oauth2Token

type Metadata =
   {
        [<JsonPropertyName("issuer")>]
        Issuer:string
        
        [<JsonPropertyName("token_endpoint")>]
        TokenEndpoint:string
        
        [<JsonPropertyName("userinfo_endpoint")>]
        UserInfoEndpoint:string
        
        [<JsonPropertyName("revocation_endpoint")>]
        RevocationEndpoint:string  
               
        [<JsonPropertyName("jwks_uri")>]
        JwksUri:string
        
        [<JsonPropertyName("device_authorization_endpoint")>]
        DeviceAuthorizationEndpoint:string 
    }
type Credentials =
    {  
        [<JsonPropertyName("type")>]
        CredsType : string
        
        [<JsonPropertyName("client_id")>]
        ClientId : string
        
        [<JsonPropertyName("client_secret")>]
        ClientSecret : string 

        [<JsonPropertyName("client_email")>]
        ClientEmail : string 

        [<JsonPropertyName("issuer_url")>]
        IssuerUrl : string 
    }

//Gets a well-known metadata URL for the given OAuth issuer URL.
//https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig
let getWellKnownMetadataUrl (issuerUrl: Uri) : Uri =
    Uri(issuerUrl.AbsoluteUri + ".well-known/openid-configuration")

let getMetadata (issuerUrl: Uri) : Async<Metadata> =
    async {
        use client = new HttpClient()
        let metadataDataUrl = getWellKnownMetadataUrl issuerUrl        
        let! response = client.GetStringAsync metadataDataUrl |> Async.AwaitTask
        return JsonSerializer.Deserialize<Metadata> response        
    }

let createClient issuerUrl =
    let data = getMetadata issuerUrl |> Async.RunSynchronously  
    TokenClient(Uri(data.TokenEndpoint))

let openAndDeserializeCreds uri =
    let text = System.IO.File.ReadAllText uri    
    JsonSerializer.Deserialize<Credentials>(text)    

type AuthenticationOauth2(issuerUrl: Uri, credentials: Uri, audience: Uri) =
    inherit Authentication()
    let tokenClient  = createClient issuerUrl
    let credentials = openAndDeserializeCreds(credentials.LocalPath) 
    let mutable token : Option<TokenResult * DateTime> = None   

    //https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
    let isTokenExpiredOrEmpty() : bool =
        match token with
        | Some v ->
            let tokenDuration = TimeSpan.FromSeconds(float (fst v).ExpiresIn)
            let tokenExpiration = (snd v).Add tokenDuration
            DateTime.Now > tokenExpiration
        | _ -> true    
      
    override this.GetAuthMethodName() = "token"
    override this.GetAuthData() =
        
        let returnTokenAsProvider () =
            AuthenticationDataToken(fun () -> (fst token.Value).AccessToken) :> AuthenticationDataProvider

        match isTokenExpiredOrEmpty () with
        | true ->
            let newToken =
                    tokenClient.exchangeClientCredentials
                       (
                        credentials.ClientId,
                        credentials.ClientSecret,
                        audience
                       )

            match newToken with
            | Result (v, d) ->
                token <- Some(v, d)
                returnTokenAsProvider()
            | OauthError e ->
                raise (
                    TokenExchangeException
                        (                        
                        e.Error
                        + Environment.NewLine
                        + e.ErrorDescription
                        + Environment.NewLine
                        + e.ErrorUri
                        )
                     )
            | OtherError e -> failwith e

        | false -> returnTokenAsProvider()
