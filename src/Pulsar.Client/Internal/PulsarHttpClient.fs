namespace Pulsar.Client.Internal

open System.Net.Http
open System.Net.Http.Json
open System.Text.Json
open System.Text.Json.Serialization
open Pulsar.Client.Api
open System

//  This class is mainly used for http lookup service
//  We name this class `PulsarHttpClient` to avoid naming clash with native HttpClient, and in Java pulsar client it's just `HttpClient`
type internal PulsarHttpClient (config: PulsarClientConfiguration) =

    let authenticationDataProvider = config.Authentication.GetAuthData()

    let jsonOptions = JsonSerializerOptions(
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
     )
    do jsonOptions.Converters.Add(JsonStringEnumConverter())

    let httpClient = new HttpClient(new SocketsHttpHandler(
        PooledConnectionLifetime = TimeSpan.FromMinutes(2),
        AllowAutoRedirect = true
    ))

    member this.Get<'T> (requestUri: string) =
        backgroundTask {
            if authenticationDataProvider.HasDataForHttp() then
                let request = new HttpRequestMessage(HttpMethod.Get, requestUri)
                for headerPropertyEntry in authenticationDataProvider.GetHttpHeaders() do
                    request.Headers.Add(headerPropertyEntry.Key, headerPropertyEntry.Value)
                let! response = httpClient.SendAsync(request)
                response.EnsureSuccessStatusCode() |> ignore
                return! response.Content.ReadFromJsonAsync<'T>(jsonOptions)

            else
                return! httpClient.GetFromJsonAsync<'T>(requestUri, jsonOptions)
        }





