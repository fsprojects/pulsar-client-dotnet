namespace Pulsar.Client.UnitTests.Common

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Common.Commands
open pulsar.proto
open Pulsar.Client.Internal

module CommandsTests =

    let private assertGetCommandType<'a> expected =
        let actual = getTypeByCommandType typeof<'a>
        let message = sprintf "Expected command type '%A' but '%A'" expected actual
        Expect.equal message expected actual

    [<Tests>]
    let tests =

        testList "CommandsTests" [

            test "getTypeByCommandType should return correct command type" {
                assertGetCommandType<CommandPartitionedTopicMetadata> CommandType.PartitionedMetadata
                assertGetCommandType<CommandConnect> CommandType.Connect
            }

            test "getTypeByCommandType should fail for unrecognized command type" {
                fun() -> getTypeByCommandType typeof<Object> |> ignore
                |> Expect.throwsT<Exception> ""
            }

            test "toBaseCommand should return correct command" {
                let topicName = "test-topic"
                let requestId = 123UL

                let command =
                    CommandPartitionedTopicMetadata(Topic = topicName, RequestId = requestId)
                    |> toBaseCommand (fun c m -> c.partitionMetadata <- m)

                command.``type``
                |> Expect.equal "" CommandType.PartitionedMetadata

                command.partitionMetadata.Topic
                |> Expect.equal "" topicName

                command.partitionMetadata.RequestId
                |> Expect.equal "" requestId
            }

            test "newPartitionMetadataRequest should return correct frame" {
                let topicName = "test-topic"
                let requestId = Generators.getNextRequestId()

                let totalSize, commandSize, command =
                    newPartitionMetadataRequest topicName requestId
                    |> fromSimpleCommandBytes

                totalSize |> Expect.equal "" 23
                commandSize |> Expect.equal "" 19

                command.partitionMetadata.Topic
                |> Expect.equal "" topicName

                command.partitionMetadata.RequestId
                |> Expect.equal "" (uint64(requestId))
            }

            test "newConnect should return correct frame" {
                let clientVersion = "client-version"
                let protocolVersion = ProtocolVersion.V1

                let totalSize, commandSize, command =
                    newConnect clientVersion protocolVersion
                    |> fromSimpleCommandBytes

                totalSize |> Expect.equal "" 26
                commandSize |> Expect.equal "" 22

                command.Connect.ClientVersion
                |> Expect.equal "" clientVersion

                command.Connect.ProtocolVersion
                |> Expect.equal "" ((int) protocolVersion)
            }
        ]