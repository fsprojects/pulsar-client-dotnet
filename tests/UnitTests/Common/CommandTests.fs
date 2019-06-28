namespace Pulsar.Client.UnitTests.Common

open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open Pulsar.Client.Common.Commands
open pulsar.proto
open Pulsar.Client.Internal
open FSharp.UMX
open System
open System.IO
open ProtoBuf
open System.Net
open System.Threading.Tasks

module CommandsTests =    
        
    let int32FromBigEndian(num : Int32) =
        IPAddress.NetworkToHostOrder(num)

    let int16FromBigEndian(num : Int16) =
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
            reader.ReadBytes(commandSize)
            |> protoDeserialize<BaseCommand>
    
        (totalSize, commandSize, command)

    let deserializePayloadCommand(bytes : byte[]) =
        use stream = new MemoryStream(bytes)
        use reader = new BinaryReader(stream)
    
        let totalSize = reader.ReadInt32() |> int32FromBigEndian
        let commandSize = reader.ReadInt32() |> int32FromBigEndian
    
        let command =
            reader.ReadBytes(commandSize)
            |> protoDeserialize<BaseCommand>

        let magicNumber = reader.ReadInt16() |> int16FromBigEndian
        let crc32 = reader.ReadInt32() |> int32FromBigEndian
        let medataSize = reader.ReadInt32() |> int32FromBigEndian

        let metadata =
            reader.ReadBytes(medataSize)
            |> protoDeserialize<MessageMetadata>

        let payload = reader.ReadBytes(bytes.Length - 8 - commandSize - 10 - medataSize)
    
        (bytes, totalSize, commandSize, command, magicNumber, crc32, medataSize, metadata, payload)

    let serializeDeserializeSimpleCommand (cmd: (MemoryStream -> Task)) = 
        let stream = new MemoryStream()
        (cmd stream).Wait() 
        let commandBytes = stream.ToArray()
        commandBytes |> deserializeSimpleCommand

    let serializeDeserializePayloadCommand (cmd: (MemoryStream -> Task)) = 
        let stream = new MemoryStream()
        (cmd stream).Wait() 
        let commandBytes = stream.ToArray()
        commandBytes |> deserializePayloadCommand

    [<Tests>]
    let tests =

        testList "CommandsTests" [

            test "newPartitionMetadataRequest should return correct frame" {
                let topicName = "test-topic"
                let requestId = Generators.getNextRequestId()
               
                let totalSize, commandSize, command = 
                    serializeDeserializeSimpleCommand (newPartitionMetadataRequest topicName requestId)

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
                    serializeDeserializeSimpleCommand (newConnect clientVersion protocolVersion)

                totalSize |> Expect.equal "" 26
                commandSize |> Expect.equal "" 22
                command.``type``  |> Expect.equal "" CommandType.Connect
                command.Connect.ClientVersion |> Expect.equal "" clientVersion
                command.Connect.ProtocolVersion |> Expect.equal "" ((int) protocolVersion)
            }

            test "newMessage should return correct frame" {
                let producerId: ProducerId =  % 5UL
                let sequenceId: SequenceId =  % 6UL
                let numMessages =  1
                let metadata = MessageMetadata(ProducerName = "TestMe")
                let payload = [| 1uy; 17uy; |]

                let (bytes, totalSize, commandSize, command, magicNumber, crc32, medataSize, resultMetadata, resultPayload) = 
                    serializeDeserializePayloadCommand (newSend producerId sequenceId numMessages metadata payload)
                
                let crcArrayStart = 8 + commandSize + 6
                let crcArray = bytes.AsSpan(crcArrayStart, 4 + medataSize + resultPayload.Length).ToArray()

                let currentCrc32 = CRC32.CRC32.Get(uint32 0, crcArray, crcArray.Length) |> int32

                magicNumber |> Expect.equal "" (int16 0x0e01)
                crc32 |> Expect.equal "" currentCrc32
            }

            test "newLookup should return correct frame" {
                let topicName = "test-topic"
                let requestId = Generators.getNextRequestId()
                let authoritative = true

                let totalSize, commandSize, command = 
                    serializeDeserializeSimpleCommand (newLookup topicName requestId authoritative )

                totalSize |> Expect.equal "" 25
                commandSize |> Expect.equal "" 21
                command.``type``  |> Expect.equal "" CommandType.Lookup
                command.lookupTopic.Topic |> Expect.equal "" topicName
                command.lookupTopic.RequestId |> Expect.equal "" (uint64(requestId))
                command.lookupTopic.Authoritative |> Expect.equal "" authoritative
            }
        ]