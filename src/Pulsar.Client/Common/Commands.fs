module Pulsar.Client.Common.Commands

open pulsar.proto
open FSharp.UMX
open ProtoBuf
open System
open System.IO
open System.Net
open pulsar.proto
open System.Data

type internal CommandType = BaseCommand.Type

let private protoSerialize instance =
    use stream = new MemoryStream()
    Serializer.Serialize(stream, instance)
    stream.ToArray()

let private protoDeserialize<'T> (bytes : byte[]) =
    use stream = new MemoryStream(bytes)
    Serializer.Deserialize<'T>(stream)

let internal int32ToBigEndian(num : Int32) =
    IPAddress.HostToNetworkOrder(num)

let internal int32FromBigEndian(num : Int32) =
    IPAddress.NetworkToHostOrder(num)

let internal toSimpleCommandBytes(command : BaseCommand) =
    let commandBytes = protoSerialize command
    let commandSize = commandBytes.Length |> int32ToBigEndian
    let totalSize = (commandBytes.Length + 4) |> int32ToBigEndian
    use stream = new MemoryStream()
    use writer = new BinaryWriter(stream)
    writer.Write(totalSize)
    writer.Write(commandSize)
    writer.Write(commandBytes)
    writer.Flush()
    stream.ToArray()

let internal fromSimpleCommandBytes(bytes : byte[]) =
    use stream = new MemoryStream(bytes)
    use reader = new BinaryReader(stream)

    let totalSize = reader.ReadInt32() |> int32FromBigEndian
    let commandSize = reader.ReadInt32() |> int32FromBigEndian

    let command =
        reader.ReadBytes(bytes.Length - 8)
        |> protoDeserialize<BaseCommand>

    (totalSize, commandSize, command)

let newPartitionMetadataRequest(topicName : string) (requestId : RequestId) : byte [] =
    let request = CommandPartitionedTopicMetadata(Topic = topicName, RequestId = uint64(%requestId))
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadata, partitionMetadata = request)
    command |> toSimpleCommandBytes

let newSend (producerId : ProducerId) (sequenceId : SequenceId)
    (numMessages : int) (checksumType : ChecksumType)
    (msgMetadata : MessageMetadata) payload : byte [] =
    [||]

let newAck (consumerId : ConsumerId) (ledgerId : LedgerId) (entryId : EntryId)
    (ackType : CommandAck.AckType) : byte [] =
    [||]

let newConnect (clientVersion: string) (protocolVersion: ProtocolVersion) : byte[] =
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int) protocolVersion)
    let command = BaseCommand(``type`` = CommandType.Connect, Connect = request)
    command |> toSimpleCommandBytes