namespace Pulsar.Client.Auth.OAuth2

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Net.Http.Headers
open System.Text.Json.Serialization
open System.Text.Json


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

type internal TokenExchangeResult =
    | Result of TokenResult * DateTime
    | OAuthError of TokenError
    | HttpError of string

type internal TokenClient (tokenUrl: Uri, client: HttpClient) =

    member this.ExchangeClientCredentials(clientId:string, clientSecret:string, audience:string, scope: string)=
        backgroundTask {
            use request = new HttpRequestMessage(HttpMethod.Post, tokenUrl)

            request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue("application/json"))
            request.Headers.Add("User-Agent", "Pulsar.Client")

            let body = seq {
                KeyValuePair("grant_type", "client_credentials")
                KeyValuePair("client_id", clientId)
                KeyValuePair("client_secret", clientSecret)
                KeyValuePair("audience", audience)
                if scope |> String.IsNullOrEmpty |> not then
                    KeyValuePair("scope", scope)
            }

            request.Content <- new FormUrlEncodedContent(body)

            use! response = client.SendAsync request
            use! resultContent = response.Content.ReadAsStreamAsync()

            match response.StatusCode with
            | HttpStatusCode.OK ->
                let! result = JsonSerializer.DeserializeAsync<TokenResult>(resultContent)
                return TokenExchangeResult.Result (result, DateTime.Now)
            | HttpStatusCode.BadRequest
            | HttpStatusCode.Unauthorized ->
                let! resultContent = response.Content.ReadAsStreamAsync()
                let! result =  JsonSerializer.DeserializeAsync<TokenError>(resultContent)
                return TokenExchangeResult.OAuthError result
            | _ ->
                return TokenExchangeResult.HttpError $"Failed to perform request to oauth server {response.StatusCode.ToString()} {response.ReasonPhrase}"
        }