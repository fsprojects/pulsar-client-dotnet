module Pulsar.Client.Auth.Oauth2Token

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Net.Http.Headers
open System.Text.Json.Serialization
open System.Text.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

type TokenError =
    {    
        [<JsonPropertyName("error")>]
        Error:string 
        
        [<JsonPropertyName("error_description")>]
        ErrorDescription:string 
        
        [<JsonPropertyName("error_uri")>]
        ErrorUri:string 
    }

type TokenResult =
    {    
        [<JsonPropertyName("access_token")>]
        AccessToken:string
        
        [<JsonPropertyName("id_token")>]
        IdToken:string
        
        [<JsonPropertyName("refresh_token")>]
        RefreshToken:string
        
        [<JsonPropertyName("expires_in")>]
        ExpiresIn:int 
    }

type TokenExchangeResult =
    | Result of TokenResult * DateTime
    | OauthError of TokenError
    | HttpError of string
    

let exchange (uri:Uri) clientId clientSecret (audience:Uri)  =
     task{
             use client = new HttpClient()            
             let request = new HttpRequestMessage(HttpMethod.Post,uri)
           
             request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue("application/json"))
             request.Headers.Add("User-Agent", "Pulsar.Client")
                         
             let body = [
                         KeyValuePair("grant_type","client_credentials")
                         KeyValuePair("client_id",clientId)
                         KeyValuePair("client_secret",clientSecret)
                         KeyValuePair("audience",audience.ToString())
                        ]           
           
             request.Content <- new FormUrlEncodedContent(body)
            
             let! response = client.SendAsync request |> Async.AwaitTask
             let! resultContent = response.Content.ReadAsStreamAsync() 
             
             match response.StatusCode with
                | HttpStatusCode.OK ->                   
                    let! result =  JsonSerializer.DeserializeAsync<TokenResult>(resultContent)
                    return TokenExchangeResult.Result (result, DateTime.Now)
                | HttpStatusCode.BadRequest
                | HttpStatusCode.Unauthorized ->
                    let! resultContent = response.Content.ReadAsStreamAsync() 
                    let! result =  JsonSerializer.DeserializeAsync<TokenError>(resultContent)
                    return TokenExchangeResult.OauthError result
                | _ -> return TokenExchangeResult.HttpError $"Failed to perform request to oauth server {response.StatusCode.ToString()} {response.ReasonPhrase}"
                                                                
          } 
     
type TokenClient(tokenUrl : Uri) =
      member this.ExchangeClientCredentials(clientId:string, clientSecret:string, audience:Uri)=
          task{
                return! exchange tokenUrl clientId clientSecret audience                
          } 
        
        
          
          
    