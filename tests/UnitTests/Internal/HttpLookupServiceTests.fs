module HttpLookupServiceTests

open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Internal

[<Tests>]
let tests =

    let withService test =
        let config = { ServiceUrl = "http://localhost:6650" }
        test (HttpLookupService config :> ILookupService)

    testList "HttpLookupService" [

        test "GetServiceUrl returns configured ServiceUrl" {
            withService <| fun(service) ->
                let actual = service.GetServiceUrl()
                let expected = "http://localhost:6650"
                Expect.equal actual expected  ""
        }

        test "UpdateServiceUrl causes service reconfiguration" {
            withService <| fun(service) ->
                let serviceUrl = "http://192.168.8.1:6650"
                service.UpdateServiceUrl serviceUrl
                let actual = service.GetServiceUrl()
                let expected = serviceUrl
                Expect.equal actual expected  ""
        }
    ]