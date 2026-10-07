namespace Pulsar.Client.Auth.OAuth2

open System
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open System.Threading.Tasks
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

type internal AuthenticationOauth2(issuerUrl: Uri, audience: string, credentialsUrl: Uri, scope: string,
                                   httpHandler: HttpMessageHandler, getTimestamp: unit -> int64) =
    inherit Authentication()

    let mutable token : Option<TokenResult * TimeStamp> = None
    let httpClient = new HttpClient(httpHandler)

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
            return TokenClient(Uri(metadata.TokenEndpoint), httpClient, getTimestamp)
        }

    let getCredsFromFile (credentialsUrl: Uri) =
        backgroundTask {
            use fs = new FileStream(credentialsUrl.LocalPath, FileMode.Open, FileAccess.Read)
            return! JsonSerializer.DeserializeAsync<Credentials>(fs)
        }

    let getCredsFromDataEncodedUri (credentialsUrl: Uri) =
        match credentialsUrl.LocalPath.Split(',', 2) with
        | [| contentType; data |] when contentType = "application/json;base64" ->
            data
            |> Convert.FromBase64String
            |> JsonSerializer.Deserialize<Credentials>
        | [| contentType; _ |] ->
            raise <| NotSupportedException $"Content type '{contentType}' is not supported."
        | _ ->
            raise <| FormatException "The credentials are not in the expected format."

    let deserializeCreds (credentialsUrl: Uri) =
        match credentialsUrl.Scheme with
        | "file" ->
            getCredsFromFile credentialsUrl
        | "data" ->
            getCredsFromDataEncodedUri credentialsUrl |> Task.FromResult
        | _ ->
            raise <| NotSupportedException($"Scheme '{credentialsUrl.Scheme}' is not supported.")

    // Brokers allow no clock skew, so a token must be replaced before `exp`, not at it.
    let earlyRefreshRatio = 0.9
    let tokenLock = obj()

    //https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
    let tryGetToken()  =
        token
        |> Option.bind (fun (tokenResult, issuedTime) ->
            let refreshAfter = TimeSpan.FromSeconds(float tokenResult.ExpiresIn * earlyRefreshRatio)
            if Stopwatch.GetElapsedTime(%issuedTime, getTimestamp()) < refreshAfter then
                Some tokenResult
            else
                None
            )

    let fetchToken() =
        let newToken =
            (backgroundTask {
                let! credentials = deserializeCreds credentialsUrl
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
            tokenResult
        | OAuthError e ->
            raise <| TokenExchangeException $"{e.Error}{Environment.NewLine} {e.ErrorDescription} {Environment.NewLine}{e.ErrorUri}"
        | HttpError e ->
            raise <| Exception e

    override this.GetAuthMethodName() =
        "token"
    override this.GetAuthData() =
        // Many connections are challenged at the same moment; the lock makes them share a single token request.
        let tokenResult =
            lock tokenLock (fun () ->
                match tryGetToken() with
                | Some tokenResult -> tokenResult
                | None -> fetchToken())
        upcast AuthenticationDataToken(fun () -> tokenResult.AccessToken)

    // https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    new(issuerUrl: Uri, audience: string, credentialsUrl: Uri, scope: string) =
        new AuthenticationOauth2(issuerUrl, audience, credentialsUrl, scope,
                                 new SocketsHttpHandler(PooledConnectionLifetime = TimeSpan.FromMinutes(2)),
                                 Stopwatch.GetTimestamp)

    override this.Dispose() =
        httpClient.Dispose()