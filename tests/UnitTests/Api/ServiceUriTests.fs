namespace Pulsar.Client.UnitTests.Api

open System

open Expecto
open Pulsar.Client.Common
open Pulsar.Client.UnitTests

module ServiceUriTests =

    [<Tests>]
    let tests =

        let unwrap (result : Result<ServiceUri, _>) =
            match result with
            | (Ok r) -> Some r
            | _ -> None

        let getAddresses result =
            (result |> unwrap |> Option.get).Addresses

        let getUseTls result =
            (result |> unwrap |> Option.get).UseTls

        let getOriginalString result =
            (result |> unwrap |> Option.get).OriginalString

        testList "ServiceUriTests" [

            test "Parse returns Error for null input" {
                let expected = Result.Error "ServiceUrl must not be blank."
                let actual = ServiceUri.parse(null)
                Expect.equal actual expected "Parse should return Error for null input"
            }

            test "Parse returns Error for empty input" {
                let expected = Result.Error "ServiceUrl must not be blank."
                let actual = ServiceUri.parse("")
                Expect.equal actual expected "Parse should return Error for empty input"
            }

            test "Parse returns Error for whitespace input" {
                let expected = Result.Error "ServiceUrl must not be blank."
                let actual = ServiceUri.parse("  ")
                Expect.equal actual expected "Parse should return Error for whitespace input"
            }

            test "Parse returns Error for unrecognized input" {
                let expected = Result.Error "Supplied string 'pulsar+zzz://host.com' describe an un-representable ServiceUri."
                let actual = ServiceUri.parse("pulsar+zzz://host.com")
                Expect.equal actual expected "Parse should return Error for unrecognized input"
            }

            test "Parse nows pulsar scheme" {
                let address = "pulsar://host:6650"
                let expected = Uri("pulsar://bassmaster:controller87@host:6650")
                let actual = address |> ServiceUri.parse |> getAddresses |> List.head
                Expect.equal actual expected "Parse should now pulsar scheme"
            }

            test "Parse nows secure pulsar scheme" {
                let address = "pulsar+ssl://host:6650"
                let expected = Uri("pulsar://host:6650")
                let actual = address |> ServiceUri.parse |> getAddresses |> List.head
                Expect.equal actual expected "Parse should now secure pulsar scheme"
            }

            test "Parse sets default port for pulsar scheme" {
                let address = "pulsar://host"
                let expected = Uri("pulsar://host:6650")
                let actual = address |> ServiceUri.parse |> getAddresses |> List.head
                Expect.equal actual expected "Parse should set default port for pulsar scheme"
            }

            test "Parse sets default port for secure pulsar scheme" {
                let address = "pulsar+ssl://host"
                let expected = Uri("pulsar://host:6651")
                let actual = address |> ServiceUri.parse |> getAddresses |> List.head
                Expect.equal actual expected "Parse should set default port for secure pulsar scheme"
            }

            test "Parse nows multiple hosts" {
                let splitters = [","; ";"]

                let serviceUris =
                    splitters
                    |> List.map(fun s -> sprintf "pulsar+ssl://host-1%shost-2:789%shost-3%shost-4:4789" s s s)

                let expected = [
                    Uri("pulsar://host-1:6651")
                    Uri("pulsar://host-2:789")
                    Uri("pulsar://host-3:6651")
                    Uri("pulsar://host-4:4789")
                ]

                serviceUris |> List.iter (fun address ->
                    let actual = address |> ServiceUri.parse |> getAddresses
                    Expect.equal actual expected "Parse should now multiple hosts"
                )
            }

            test "Parse drops user info" {
                let address = "pulsar://user:password@host:6650"
                let expected = Uri("pulsar://host:6650")
                let actual = address |> ServiceUri.parse |> getAddresses |> List.head
                Expect.equal actual.AbsoluteUri expected.AbsoluteUri "Parse should drop user info part"
            }

            test "UseTls false if 'ssl' service not specified" {
                let address = "pulsar://host"
                let useTls = address |> ServiceUri.parse |> getUseTls
                Expect.isFalse useTls "UseTls should be false if 'ssl' service not specified"
            }

            test "UseTls true if 'ssl' service specified" {
                let address = "pulsar+ssl://host"
                let useTls = address |> ServiceUri.parse |> getUseTls
                Expect.isTrue useTls "UseTls should be true if 'ssl' service specified"
            }

            test "'OriginalString' same as input string" {
                let address = "pulsar+ssl://host-1;host-2:789;host-3;host-4:4789"
                let actual = address |> ServiceUri.parse |> getOriginalString
                Expect.equal actual address "'OriginalString' should be same as input string"
            }
        ]