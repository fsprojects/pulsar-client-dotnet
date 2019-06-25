namespace Pulsar.Client.UnitTests.Common

open Expecto
open Expecto.Flip
open Pulsar.Client.Common.Commands
open pulsar.proto
open Pulsar.Client.Internal
open System.Data
open System.Buffers
open System
open System.IO
open ProtoBuf
open System.Net
open System.Threading.Tasks

module CommandsTests =    
        
    let int32FromBigEndian(num : Int32) =
        IPAddress.NetworkToHostOrder(num)
    
    let private protoDeserialize<'T> (bytes : byte[]) =
        use stream = new MemoryStream(bytes)
        Serializer.Deserialize<'T>(stream)
    
    let deserializeSimpleCommand(bytes : byte[]) =
        use stream = new MemoryStream(bytes)
        use reader = new BinaryReader(stream)
    
        let totalSize = reader.ReadInt32() |> int32FromBigEndian
        let commandSize = reader.ReadInt32() |> int32FromBigEndian
    
        let command =
            reader.ReadBytes(bytes.Length - 8)
            |> protoDeserialize<BaseCommand>
    
        (totalSize, commandSize, command)

    let serializeDeserialize (cmd: (MemoryStream -> Task)) = 
        let stream = new MemoryStream()
        (cmd stream).Wait() 
        let commandBytes = stream.ToArray()
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

            test "newLookup should return correct frame" {
                let topicName = "test-topic"
                let requestId = Generators.getNextRequestId()
                let authoritative = true

                let totalSize, commandSize, command = 
                    serializeDeserialize (newLookup topicName requestId authoritative )

                totalSize |> Expect.equal "" 26
                commandSize |> Expect.equal "" 22
                command.``type``  |> Expect.equal "" CommandType.Connect
                command.lookupTopic.Topic |> Expect.equal "" topicName
                command.lookupTopic.RequestId |> Expect.equal "" (uint64(requestId))
                command.lookupTopic.Authoritative |> Expect.equal "" authoritative
            }
        ]