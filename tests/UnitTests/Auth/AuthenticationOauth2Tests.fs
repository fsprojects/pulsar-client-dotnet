module Pulsar.Client.UnitTests.Auth.AuthenticationOauth2Tests

open System
open System.Diagnostics
open System.Net
open System.Net.Http
open System.Text
open System.Threading
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Auth.OAuth2

let private issuerUrl = Uri("https://issuer.test/")
let private tokenEndpoint = "https://issuer.test/oauth/token"
let private expiresInSeconds = 100

let private credentialsUrl =
    let json = """{"type":"client_credentials","client_id":"id","client_secret":"secret","issuer_url":"https://issuer.test/"}"""
    Uri("data:application/json;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes json))

type private FakeClock() =
    let mutable now = 0L
    member this.GetTimestamp() = Interlocked.Read(&now)
    member this.Advance(seconds: float) =
        Interlocked.Add(&now, int64 (seconds * float Stopwatch.Frequency)) |> ignore

type private FakeIssuer(clock: FakeClock, requestDuration: float, responseDelay: TimeSpan) =
    inherit HttpMessageHandler()
    let mutable tokenRequests = 0
    member this.TokenRequests = Volatile.Read(&tokenRequests)
    override this.SendAsync(request, _) =
        backgroundTask {
            let json =
                if request.RequestUri.AbsolutePath.EndsWith("/.well-known/openid-configuration") then
                    $"""{{"issuer":"{issuerUrl}","token_endpoint":"{tokenEndpoint}"}}"""
                else
                    let n = Interlocked.Increment(&tokenRequests)
                    clock.Advance requestDuration
                    $"""{{"access_token":"token-{n}","expires_in":{expiresInSeconds}}}"""
            if responseDelay > TimeSpan.Zero then
                do! Task.Delay responseDelay
            return new HttpResponseMessage(HttpStatusCode.OK, Content = new StringContent(json, Encoding.UTF8, "application/json"))
        }

let private createAuth (requestDuration: float) (responseDelay: TimeSpan) =
    let clock = FakeClock()
    let issuer = new FakeIssuer(clock, requestDuration, responseDelay)
    let auth = new AuthenticationOauth2(issuerUrl, "audience", credentialsUrl, "", issuer, clock.GetTimestamp)
    auth, issuer, clock

let private currentToken (auth: AuthenticationOauth2) =
    auth.GetAuthData().GetCommandData()

[<Tests>]
let tests =
    testList "AuthenticationOauth2" [

        test "Reuses the cached token before the early refresh point" {
            let auth, issuer, clock = createAuth 0.0 TimeSpan.Zero
            currentToken auth |> Expect.equal "" "token-1"
            clock.Advance 89.0
            currentToken auth |> Expect.equal "" "token-1"
            issuer.TokenRequests |> Expect.equal "" 1
        }

        test "Refreshes the token before it expires" {
            let auth, issuer, clock = createAuth 0.0 TimeSpan.Zero
            currentToken auth |> Expect.equal "" "token-1"
            clock.Advance 91.0
            currentToken auth |> Expect.equal "" "token-2"
            issuer.TokenRequests |> Expect.equal "" 2
        }

        test "Measures token lifetime from before the token request" {
            // The issuer sets exp when it handles the request, so a slow response must not extend the local lifetime.
            let auth, _, clock = createAuth 5.0 TimeSpan.Zero
            currentToken auth |> Expect.equal "" "token-1"
            clock.Advance 86.0
            currentToken auth |> Expect.equal "" "token-2"
        }

        // GetAuthData blocks its caller, so this runs alone to avoid starving the thread pool for other tests.
        testSequenced <| testTask "Concurrent callers share a single token request" {
            let auth, issuer, _ = createAuth 0.0 (TimeSpan.FromMilliseconds 100.0)
            let! tokens =
                Array.init 8 (fun _ -> Task.Run(fun () -> currentToken auth))
                |> Task.WhenAll
            tokens |> Array.distinct |> Expect.equal "" [| "token-1" |]
            issuer.TokenRequests |> Expect.equal "" 1
        }
    ]
