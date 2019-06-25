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
open Microsoft.IO
open System.IO.Pipelines
open Pipelines.Sockets.Unofficial
open System.Threading.Tasks

type internal CommandType = BaseCommand.Type

let private memoryStreamManager = RecyclableMemoryStreamManager()
Serializer.PrepareSerializer<BaseCommand>()

let inline int32ToBigEndian(num : Int32) =
    IPAddress.HostToNetworkOrder(num)

let internal serializeSimpleCommand(command : BaseCommand) =
    fun (output: Stream) ->
        use stream = memoryStreamManager.GetStream()
        // write fake totalLength
        for i in 1..8 do
            stream.WriteByte(0uy)
        Serializer.Serialize(stream, command)
        let frameSize = int stream.Length  
        let totalSize = frameSize - 4 
        let commandSize = frameSize - 8

        stream.Seek(0L,SeekOrigin.Begin) |> ignore
        use binaryWriter = new BinaryWriter(stream)
        binaryWriter.Write(int32ToBigEndian totalSize)
        binaryWriter.Write(int32ToBigEndian commandSize)
        stream.Seek(0L, SeekOrigin.Begin) |> ignore
                
        stream.CopyToAsync(output)

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