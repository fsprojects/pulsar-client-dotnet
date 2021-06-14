module Pulsar.Client.Auth.Oauth2

open System
open System.IO
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Auth.Oauth2Token
open FSharp.Control.Tasks.V2.ContextInsensitive
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

let getMetadata (issuerUrl: Uri)  =
    task {
        let client = new HttpClient()        
        let metadataDataUrl = getWellKnownMetadataUrl issuerUrl        
        let! response = client.GetStreamAsync metadataDataUrl
        let! deserialized = JsonSerializer.DeserializeAsync<Metadata> response
        return (client,deserialized)         
    }
let createClient issuerUrl =
    task {
        let! client,data = getMetadata issuerUrl 
        return new TokenClient(Uri(data.TokenEndpoint),client)
    }
    
let openAndDeserializeCreds uri =
    task{
        use fs = new FileStream(uri, FileMode.Open)
        let! temp = JsonSerializer.DeserializeAsync<Credentials>(fs)
        return temp
    }
    
type AuthenticationOauth2(issuerUrl: Uri, privateKey: Uri, audience: string) =  
    inherit Authentication()  
    let tokenClientTask  = createClient issuerUrl     
    let mutable token : Option<TokenResult * DateTime> = None  

    //https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
    let isTokenExpiredOrEmpty() : bool =
        match token with
        | Some (res, exp) ->
            let tokenDuration = TimeSpan.FromSeconds(float res.ExpiresIn)
            let tokenExpiration = exp.Add tokenDuration
            DateTime.Now > tokenExpiration
        | _ -> true
        
    override this.GetAuthMethodName() = "token"
    override this.GetAuthData() =
        
            let returnTokenAsProvider (token:TokenResult) =
              AuthenticationDataToken(fun () -> token.AccessToken) :> AuthenticationDataProvider
                   
            match isTokenExpiredOrEmpty () with
            | true ->
                let prettyTask = task{
                    let! credentials = openAndDeserializeCreds(privateKey.LocalPath)
                    let! tokenTaskResult = tokenClientTask
                    
                    let newToken =
                                tokenTaskResult.ExchangeClientCredentials
                                   (
                                    credentials.ClientId,
                                    credentials.ClientSecret,
                                    audience
                                   )
                    return! newToken                
                }
                let newToken = prettyTask.GetAwaiter().GetResult()
                match newToken with
                | Result (v, d) ->
                    token <- Some(v, d)
                    returnTokenAsProvider(v)
                | OauthError e ->  TokenExchangeException $"{e.Error}{Environment.NewLine} {e.ErrorDescription} {Environment.NewLine}{e.ErrorUri}" |> raise                      
                | HttpError  e ->  Exception(e) |> raise

            | false ->  returnTokenAsProvider(fst(token.Value))
              
    interface IDisposable with
          member this.Dispose() =
              let token =tokenClientTask.GetAwaiter().GetResult()
              (token :> IDisposable).Dispose()
              