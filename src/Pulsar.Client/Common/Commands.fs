module Pulsar.Client.Common.Commands

open pulsar.proto
open FSharp.UMX
open ProtoBuf
open System
open System.IO
open System.Net
open pulsar.proto
open System.Data
open System.Buffers.Binary

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

let internal serializeSimpleCommand(command : BaseCommand) =
    fun (buffer: Memory<byte>) ->
        let commandBytes = protoSerialize command
    
        let commandSize = commandBytes.Length
        let totalSize = commandBytes.Length + 4
        let frameSize = totalSize + 4
      
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span, totalSize)
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4), commandSize)
        commandBytes.CopyTo(buffer.Span.Slice(8))
        frameSize

let internal deserializeSimpleCommand(bytes : byte[]) =
    use stream = new MemoryStream(bytes)
    use reader = new BinaryReader(stream)

    let totalSize = reader.ReadInt32() |> int32FromBigEndian
    let commandSize = reader.ReadInt32() |> int32FromBigEndian

    let command =
        reader.ReadBytes(bytes.Length - 8)
        |> protoDeserialize<BaseCommand>

    (totalSize, commandSize, command)

let newPartitionMetadataRequest(topicName : string) (requestId : RequestId) : SerializedPayload =
    let request = CommandPartitionedTopicMetadata(Topic = topicName, RequestId = uint64(%requestId))
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadata, partitionMetadata = request)
    command |> serializeSimpleCommand

let newSend (producerId : ProducerId) (sequenceId : SequenceId)
    (numMessages : int) (checksumType : ChecksumType)
    (msgMetadata : MessageMetadata) payload : SerializedPayload =
    Unchecked.defaultof<SerializedPayload>

let newAck (consumerId : ConsumerId) (ledgerId : LedgerId) (entryId : EntryId)
    (ackType : CommandAck.AckType) : SerializedPayload =
    Unchecked.defaultof<SerializedPayload>

let newConnect (clientVersion: string) (protocolVersion: ProtocolVersion) : SerializedPayload =
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int) protocolVersion)
    let command = BaseCommand(``type`` = CommandType.Connect, Connect = request)
    command |> serializeSimpleCommand