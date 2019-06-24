namespace Pulsar.Client.UnitTests.Common

open Expecto
open Expecto.Flip
open Pulsar.Client.Common.Commands
open pulsar.proto
open Pulsar.Client.Internal
open System.Data
open System.Buffers
open System

module CommandsTests =

    let serializeDeserialize cmd = 
        let rentedArray = ArrayPool<byte>.Shared.Rent(30)
        let memory = Memory(rentedArray)
        let frameSize = cmd memory
        let commandBytes = memory.Span.Slice(0, frameSize).ToArray()
        ArrayPool<byte>.Shared.Return(rentedArray)
        commandBytes |> deserializeSimpleCommand

    [<Tests>]
    let tests =

        testList "CommandsTests" [

            test "newPartitionMetadataRequest should return correct frame" {
                let topicName = "test-topic"
                let requestId = Generators.getNextRequestId()
               
                let totalSize, commandSize, command = 
                    serializeDeserialize (newPartitionMetadataRequest topicName requestId)

                totalSize |> Expect.equal "" 23
                commandSize |> Expect.equal "" 19
                command.``type``  |> Expect.equal "" CommandType.PartitionedMetadata
                command.partitionMetadata.Topic |> Expect.equal "" topicName
                command.partitionMetadata.RequestId |> Expect.equal "" (uint64(requestId))
            }

            test "newConnect should return correct frame" {
                let clientVersion = "client-version"
                let protocolVersion = ProtocolVersion.V1

                let totalSize, commandSize, command = 
                    serializeDeserialize (newConnect clientVersion protocolVersion)

                totalSize |> Expect.equal "" 26
                commandSize |> Expect.equal "" 22
                command.``type``  |> Expect.equal "" CommandType.Connect
                command.Connect.ClientVersion |> Expect.equal "" clientVersion
                command.Connect.ProtocolVersion |> Expect.equal "" ((int) protocolVersion)
            }
        ]