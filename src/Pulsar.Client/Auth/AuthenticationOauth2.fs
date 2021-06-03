module Pulsar.Client.Auth.Oauth2

open System
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Auth.Metadata
open Pulsar.Client.Auth.Token

type Credentials(clientId: string, clientSecret: string, clientEmail: string, issuer: string) =
    new() = Credentials(String.Empty, String.Empty, String.Empty, String.Empty)

    [<JsonPropertyName("type")>]
    member x.credsType : string = "client_credentials"

    [<JsonPropertyName("client_id")>]
    member x.clientId : string = clientId

    [<JsonPropertyName("client_secret")>]
    member x.clientSecret : string = clientSecret

    [<JsonPropertyName("client_email")>]
    member x.clientEmail : string = clientEmail

    [<JsonPropertyName("issuer_url")>]
    member x.issuerUrl : string = issuer


//Gets a well-known metadata URL for the given OAuth issuer URL.
//https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig
let getWellKnownMetadataUrl (issuerUrl: Uri) : Uri =
    Uri(
        issuerUrl.AbsoluteUri
        + "/.well-known/openid-configuration"
    )

let getMetaData (issuerUrl: Uri) : Async<Metadata> =
    async {
        use client = new HttpClient()
        let metadataDataUrl = getWellKnownMetadataUrl issuerUrl

        let! response =
            client.GetStringAsync metadataDataUrl
            |> Async.AwaitTask

        return JsonSerializer.Deserialize<Metadata>(response)
    }

let createClient issuerUrl =

    let data =
        getMetaData issuerUrl |> Async.RunSynchronously

    let tokenUrl = data.tokenEndpoint
    TokenClient(Uri(tokenUrl))





type AuthenticationOauth2(issuerUrl: Uri, credentials: Credentials, audience: Uri) =
    inherit Authentication()

    let mutable _token : Option<TokenResult * DateTime> = None

    let isTokenExpiredOrEmpty () : bool =
        match _token with
        | Some v ->
            let tokenDuration =
                TimeSpan.FromSeconds(float (fst v).expiresIn)

            let tokenExpiration = (snd v).Add tokenDuration
            DateTime.Now > tokenExpiration
        | _ -> false

    member x.token
        with get () = _token
        and set value = _token <- value

    member x.tokenClient : TokenClient = createClient (issuerUrl)

    member x.credentials : Credentials = credentials

    override this.GetAuthMethodName() = "token"


    override this.GetAuthData() =
        let returnTokenAsProvider () =
            AuthenticationDataToken(fun () -> (fst _token.Value).accessToken) :> AuthenticationDataProvider

        match isTokenExpiredOrEmpty () with
        | true ->
            let newToken =
                this.tokenClient.exchangeClientCredentials (
                    this.credentials.clientId,
                    this.credentials.clientSecret,
                    audience
                )

            match newToken with
            | Result (v, d) ->
                this.token <- Some(v, d)
                returnTokenAsProvider ()
            | OauthError e ->
                raise (
                    TokenExchangeException(
                        e.error
                        + Environment.NewLine
                        + e.errorDescription
                        + Environment.NewLine
                        + e.errorUri
                    )
                )
            | OtherError e -> failwith e


        | false -> returnTokenAsProvider ()
