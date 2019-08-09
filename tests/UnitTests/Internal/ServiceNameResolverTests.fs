module Pulsar.Client.UnitTests.Internal.ServiceNameResolverTests

open System
open System.Net
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Internal


[<Tests>]
let tests =

    let withTestData test =
        let config = { ServiceUrl = "pulsar://localhost:6650"; OperationTimeout = TimeSpan.Zero }
        let resolver = ServiceNameResolver config
        test(config, resolver)

    testList "ServiceNameResolver" [

        test "GetServiceUri returns ServiceUri from config" {
            withTestData <| fun(config, resolver) ->
                resolver.GetServiceUri()
                |> Expect.equal "" (ServiceUri(config.ServiceUrl))
        }

        test "GetServiceUrl returns ServiceUrl from config" {
            withTestData <| fun(config, resolver) ->
                resolver.GetServiceUrl()
                |> Expect.equal "" config.ServiceUrl
        }

        test "UpdateServiceUrl updates resolver configuration" {
            withTestData <| fun(_, resolver) ->
                resolver.UpdateServiceUrl("pulsar://127.0.0.1:6650")
                |> Expect.equal "" (resolver.GetServiceUrl())
        }

        test "ResolveHost creates endpoint from config" {
            withTestData <| fun(_, resolver) ->
                resolver.ResolveHost()
                |> Expect.equal "" (DnsEndPoint("localhost", 6650))
        }

        test "ResolveHostUri creates ServiceUri from config" {
            withTestData <| fun(config, resolver) ->
                resolver.ResolveHostUri()
                |> Expect.equal "" (Uri(config.ServiceUrl))
        }
    ]