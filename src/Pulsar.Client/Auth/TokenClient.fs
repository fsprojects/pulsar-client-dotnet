module Pulsar.Client.Auth.Token

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Net.Http.Headers
open System.Text.Json.Serialization
open System.Text.Json

type TokenError = {
    [<JsonPropertyName("error")>]
    error:string 
    
    [<JsonPropertyName("error_description")>]
    errorDescription:string 
    
    [<JsonPropertyName("error_uri")>]
    errorUri:string 
    }

type TokenResult ={
    [<JsonPropertyName("access_token")>]
    accessToken:string     
    [<JsonPropertyName("id_token")>]
    idToken:string     
    [<JsonPropertyName("refresh_token")>]
    refreshToken:string     
    [<JsonPropertyName("expires_in")>]
    expiresIn:int 
    }
type TokenExchangeResult =
    | Result of TokenResult*DateTime
    | OauthError of TokenError
    | OtherError of string
//curl --request POST \
//  --url https://dev-kt-aa9ne.us.auth0.com \
//  --header 'content-type: application/json' \
//  --data '{
//  "client_id":"Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
//  "client_secret":"rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
//  "audience":"https://dev-kt-aa9ne.us.auth0.com/api/v2/",
//  "grant_type":"client_credentials"}'
let Exchange (uri:Uri,clientId:string, clientSecret:string, audience:Uri) : TokenExchangeResult=
     async{
             use client = new HttpClient()
             //let userAgent =  ProductInfoHeaderValue("Pulsar-Java-v2.7.1")  //where to get current ver?
             //...setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
             
             let request = new HttpRequestMessage(HttpMethod.Post,uri)
            // request.Headers.UserAgent.Add userAgent
             request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue("application/json"))
             
             request.Headers.Add("User-Agent","Pulsar-Java-v2.7.1")
             let body = [KeyValuePair("grant_type","client_credentials");
                         KeyValuePair("client_id",clientId);
                         KeyValuePair("client_secret",clientSecret);
                         KeyValuePair("audience",audience.ToString())]           
           
             request.Content <- new FormUrlEncodedContent(body)
            // request.Content.Headers.Add("Content-Type","application/x-www-form-urlencoded")
             let! response = client.SendAsync request |> Async.AwaitTask
             let! resultContent = response.Content.ReadAsStringAsync() |> Async.AwaitTask
             match response.StatusCode with
                | HttpStatusCode.OK ->                   
                    let result =  JsonSerializer.Deserialize<TokenResult>(resultContent)
                    return TokenExchangeResult.Result (result, DateTime.Now)
                | HttpStatusCode.BadRequest
                | HttpStatusCode.Unauthorized ->
                    let! resultContent = response.Content.ReadAsStringAsync() |> Async.AwaitTask
                    let result =  JsonSerializer.Deserialize<TokenError>(resultContent)
                    return TokenExchangeResult.OauthError result
                | _ -> return TokenExchangeResult.OtherError ("Failed to perform request to oauth server "
                                                              + response.StatusCode.ToString()
                                                              + " " +    response.ReasonPhrase)   
          } |> Async.RunSynchronously
     
type TokenClient(tokenUrl : Uri) =
      member x.exchangeClientCredentials(clientId:string, clientSecret:string, audience:Uri)=
          Exchange (tokenUrl,clientId,clientSecret,audience)
          
    