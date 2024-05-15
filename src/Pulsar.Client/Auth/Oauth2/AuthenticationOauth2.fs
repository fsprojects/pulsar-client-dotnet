namespace Pulsar.Client.Auth.OAuth2

open System
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Common
open FSharp.UMX

open Pulsar.Client.Auth
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

type internal AuthenticationOauth2(issuerUrl: Uri, audience: string, privateKey: Uri, scope: string) =
    inherit Authentication()

    let mutable token : Option<TokenResult * TimeStamp> = None
    // https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    let httpClient = new HttpClient(new SocketsHttpHandler(PooledConnectionLifetime = TimeSpan.FromMinutes(2)))

    //Gets a well-known metadata URL for the given OAuth issuer URL.
    //https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig
    let getWellKnownMetadataUrl (issuerUrl: Uri) : Uri =
        Uri(issuerUrl.AbsoluteUri + ".well-known/openid-configuration")

    let getMetadata (httpClient: HttpClient) (issuerUrl: Uri)  =
        backgroundTask {
            let metadataDataUrl = getWellKnownMetadataUrl issuerUrl
            let! response = httpClient.GetStreamAsync metadataDataUrl
            return! JsonSerializer.DeserializeAsync<Metadata> response
        }
    let getTokenClient() =
        backgroundTask {
            let! metadata = getMetadata httpClient issuerUrl
            return TokenClient(Uri(metadata.TokenEndpoint), httpClient)
        }

    let openAndDeserializeCreds uri =
        backgroundTask {
            use fs = new FileStream(uri, FileMode.Open, FileAccess.Read)
            let! temp = JsonSerializer.DeserializeAsync<Credentials>(fs)
            return temp
        }

    let base64DeserializeCreds data =
        data
        |> Convert.FromBase64String
        |> System.Text.Encoding.UTF8.GetString
        |> JsonSerializer.Deserialize<Credentials>

    let decodeAndDeserializeCreds (privateKey: Uri) =
        let parts = privateKey.LocalPath.Split(',', 2)
        match parts with
        | [| contentType; data |] when contentType = "application/json;base64" ->
            base64DeserializeCreds data
        | [| contentType; _ |] ->
            raise <| NotSupportedException $"Content type '{contentType}' is not supported."
        | _ ->
            raise <| FormatException "The private key is not in the expected format."

    let deserializeCreds (privateKey: Uri) =
        backgroundTask {
            match privateKey.Scheme.ToLowerInvariant() with
            | "file" ->
                return! openAndDeserializeCreds(privateKey.LocalPath)
            | "data" ->
                return decodeAndDeserializeCreds(privateKey)
            | _ ->
                return NotSupportedException($"Scheme '{privateKey.Scheme}' is not supported.") |> raise
        }

    //https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
    let tryGetToken()  =
        token
        |> Option.bind (fun (tokenResult, issuedTime) ->
            let tokenDuration = TimeSpan.FromSeconds(float tokenResult.ExpiresIn)
            if Stopwatch.GetElapsedTime(%issuedTime) < tokenDuration then
                Some tokenResult
            else
                None
            )


    override this.GetAuthMethodName() =
        "token"
    override this.GetAuthData() =

        match tryGetToken() with
        | None ->
            let newToken =
                (backgroundTask {
                    let! credentials = deserializeCreds(privateKey)
                    let! tokenClient = getTokenClient()
                    return!
                        tokenClient.ExchangeClientCredentials(
                            credentials.ClientId,
                            credentials.ClientSecret,
                            audience,
                            scope
                        )
                }).GetAwaiter().GetResult()
            match newToken with
            | Result (tokenResult, issuedTime) ->
                token <- Some(tokenResult, issuedTime)
                upcast AuthenticationDataToken(fun () -> tokenResult.AccessToken)
            | OAuthError e ->
                raise <| TokenExchangeException $"{e.Error}{Environment.NewLine} {e.ErrorDescription} {Environment.NewLine}{e.ErrorUri}"
            | HttpError e ->
                raise <| Exception e

        | Some tokenResult ->
            upcast AuthenticationDataToken(fun () -> tokenResult.AccessToken)