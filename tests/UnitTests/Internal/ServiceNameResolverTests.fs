module ServiceNameResolverTests

open System
open System.Net
open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Internal


[<Tests>]
let tests =

    let config = { ServiceUrl = "pulsar://localhost:6650" }
    let resolver = ServiceNameResolver config

    testList "ServiceNameResolver" [

        test "GetServiceUri returns ServiceUri from config" {
            let actual = resolver.GetServiceUri()
            let expected = ServiceUri(config.ServiceUrl)
            Expect.equal actual expected  ""
        }

        test "GetServiceUrl returns ServiceUrl from config" {
            let actual = resolver.GetServiceUrl()
            let expected = config.ServiceUrl
            Expect.equal actual expected  ""
        }

        test "UpdateServiceUrl creates resolver with new ServiceUrl" {
            let config' = { config with ServiceUrl = "pulsar://127.0.0.1:6650" }
            let actual = resolver.UpdateServiceUrl(config'.ServiceUrl).GetServiceUrl()
            let expected = config'.ServiceUrl
            Expect.equal actual expected  ""
        }

        test "ResolveHost creates endpoint from config" {
            let actual = resolver.ResolveHost()
            let expected = DnsEndPoint("localhost", 6650)
            Expect.equal actual expected  ""
        }

        test "ResolveHostUri creates ServiceUri from config" {
            let actual = resolver.ResolveHostUri()
            let expected = Uri(config.ServiceUrl)
            Expect.equal actual expected  ""
        }
    ]