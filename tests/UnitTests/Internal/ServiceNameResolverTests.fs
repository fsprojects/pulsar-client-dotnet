module ServiceNameResolverTests

open System
open System.Net
open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Internal

let private config = { ServiceUrl = "pulsar://localhost:6650" }

[<Tests>]
let tests =
    let resolver = ServiceNameResolver config

    testList "ServiceNameResolver" [

        testCase "GetServiceUri returns ServiceUri from config" <| fun() ->
            let actual = resolver.GetServiceUri()
            let expected = ServiceUri(config.ServiceUrl)
            Expect.equal actual expected  ""

        testCase "GetServiceUrl returns ServiceUrl from config" <| fun() ->
            let actual = resolver.GetServiceUrl()
            let expected = config.ServiceUrl
            Expect.equal actual expected  ""

        testCase "UpdateServiceUrl creates resolver with new ServiceUrl" <| fun() ->
            let config' = { config with ServiceUrl = "pulsar://127.0.0.1:6650" }
            let actual = resolver.UpdateServiceUrl(config'.ServiceUrl).GetServiceUrl()
            let expected = config'.ServiceUrl
            Expect.equal actual expected  ""

        testCase "ResolveHost creates endpoint from config" <| fun() ->
            let actual = resolver.ResolveHost()
            let expected = DnsEndPoint("localhost", 6650)
            Expect.equal actual expected  ""

        testCase "ResolveHostUri creates ServiceUri from config" <| fun() ->
            let actual = resolver.ResolveHostUri()
            let expected = Uri(config.ServiceUrl)
            Expect.equal actual expected  ""
    ]