module Pulsar.Client.Auth.Oauth2

open System
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Auth.Metadata    
open Pulsar.Client.Auth.Token

type Credentials =
    [<JsonPropertyName("type")>]
    member x.credsType:string =  String.Empty
    
    [<JsonPropertyName("client_id")>]
    member x.clientId:string =  String.Empty
    
    [<JsonPropertyName("client_secret")>]
    member x.clientSecret:string =  String.Empty
    
    [<JsonPropertyName("client_email")>]
    member x.clientEmail:string =  String.Empty
    
    [<JsonPropertyName("issuer_url")>]
    member x.issuerUrl:string =  String.Empty    
   
  
//Gets a well-known metadata URL for the given OAuth issuer URL.
//https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig   
let getWellKnownMetadataUrl (issuerUrl : Uri) : Uri =
         Uri (issuerUrl.AbsoluteUri + "/.well-known/openid-configuration")    

let getMetaData (issuerUrl :Uri): Async<Metadata> =
         async{
              use client = new HttpClient()
              let metadataDataUrl = getWellKnownMetadataUrl issuerUrl
              let! response = client.GetStringAsync metadataDataUrl |> Async.AwaitTask
              return  JsonSerializer.Deserialize<Metadata>(response)              
         }    
   
let createClient issuerUrl =        //0 (create class) this.flow = ClientCredentialsFlow.fromParameters(params);
         //looks like I can omit it
         //1 (oauth server) this.metadata = DefaultMetadataResolver.fromIssuerUrl(issuerUrl).resolve();         
         let data = getMetaData issuerUrl |> Async.RunSynchronously
         
         //2 (token from metadata) URL tokenUrl = this.metadata.getTokenEndpoint();
         let tokenUrl = data.tokenEndpoint
         //3 (create class) this.exchanger = new TokenClient(tokenUrl);
         TokenClient(Uri(tokenUrl))      



    
//credentialsJson is a json File    
type AuthenticationOauth2 (issuerUrl : Uri, credentialsJson: string, audience: Uri) =     
     inherit Authentication()
     
     let mutable _token : Option<TokenResult*DateTime> = None
     
     let isTokenExpiredOrEmpty() : bool =
          match _token with
          | Some v ->
              let tokenDuration = TimeSpan.FromSeconds (float (fst v).expiresIn)
              let tokenExpiration = (snd v).Add tokenDuration
              DateTime.Now > tokenExpiration
          | _ -> false

     member  x.token 
        with get() = _token
        and set value =  _token <- value
        
     member x.tokenClient : TokenClient = createClient(issuerUrl)
     
     member x.credentials : Credentials = JsonSerializer.Deserialize<Credentials>(credentialsJson)
     
     override this.GetAuthMethodName() =
        "token"
     
    
     override this.GetAuthData() =
        let returnTokenAsProvider ()=
           Pulsar.Client.Auth.AuthenticationDataToken( fun ()-> (fst _token.Value).accessToken)
                             :> AuthenticationDataProvider
                             
        match isTokenExpiredOrEmpty() with
        | true ->
            let newToken = this.tokenClient.exchangeClientCredentials(this.credentials.clientId,
                                                           this.credentials.clientSecret, audience)
            match newToken with
            | Result (v,d) -> this.token <- Some (v,d)
                              returnTokenAsProvider()
            | OauthError e -> failwith (e.error + e.errorDescription + e.errorUri)
            | OtherError e -> failwith e
            
                  
        | false ->    returnTokenAsProvider()

        