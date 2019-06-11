module HttpLookupServiceTests

open Expecto
open Pulsar.Client.Api
open Pulsar.Client.Internal

[<Tests>]
let tests =

    let withService test =
        let config = { ServiceUrl = "pulsar://localhost:6650" }
        test (BinaryLookupService config)

    testList "BinaryLookupService" [

        test "GetServiceUrl returns configured ServiceUrl" {
            withService <| fun(service) ->
                let actual = service.GetServiceUrl()
                let expected = "pulsar://localhost:6650"
                Expect.equal actual expected  ""
        }

        test "UpdateServiceUrl causes service reconfiguration" {
            withService <| fun(service) ->
                let serviceUrl = "pulsar://192.168.8.1:6650"
                service.UpdateServiceUrl serviceUrl
                let actual = service.GetServiceUrl()
                let expected = serviceUrl
                Expect.equal actual expected  ""
        }
    ]