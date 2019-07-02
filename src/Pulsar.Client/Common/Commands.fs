module Pulsar.Client.Common.Commands

open pulsar.proto
open FSharp.UMX
open ProtoBuf
open System
open System.IO
open System.Net
open Microsoft.IO
open CRC32

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

        // write commandPayload
        Serializer.Serialize(stream, command)
        let frameSize = int stream.Length  

        let totalSize = frameSize - 4 
        let commandSize = totalSize - 4

        //write total size and command size
        stream.Seek(0L,SeekOrigin.Begin) |> ignore
        use binaryWriter = new BinaryWriter(stream)
        binaryWriter.Write(int32ToBigEndian totalSize)
        binaryWriter.Write(int32ToBigEndian commandSize)
        stream.Seek(0L, SeekOrigin.Begin) |> ignore

        stream.CopyToAsync(output)

let internal serializePayloadCommand (command : BaseCommand) (metadata: MessageMetadata) (payload: byte[]) =
    fun (output: Stream) ->
        use stream = memoryStreamManager.GetStream()

        // write fake totalLength
        for i in 1..8 do
            stream.WriteByte(0uy)

        // write commandPayload
        Serializer.Serialize(stream, command)

        let stream1Size = int stream.Length  
        let totalCommandSize = stream1Size - 4 
        let commandSize = totalCommandSize - 4

        // write magic number 0x0e01
        stream.WriteByte(14uy)
        stream.WriteByte(1uy)

        // write fake CRC sum and fake metadata length
        for i in 1..8 do
            stream.WriteByte(0uy)
        
        // write metadata
        Serializer.Serialize(stream, metadata)
        let stream2Size = int stream.Length
        let totalMetadataSize = stream2Size - stream1Size - 6
        let metadataSize = totalMetadataSize - 4 

        // write payload
        stream.Write(payload, 0, payload.Length)
        
        let frameSize = int stream.Length  
        let totalSize = frameSize - 4 
        let payloadSize = frameSize - stream2Size

        let crcStart = stream1Size + 2
        let crcPayloadStart = crcStart + 4       

        // write missing sizes
        use binaryWriter = new BinaryWriter(stream)

        //write Metadata size
        stream.Seek(int64 crcPayloadStart, SeekOrigin.Begin) |> ignore
        binaryWriter.Write(int32ToBigEndian metadataSize)

        //write CRC
        stream.Seek(int64 crcPayloadStart, SeekOrigin.Begin) |> ignore
        let crc = int32 <| CRC32C.Get(0u, stream, totalMetadataSize + payloadSize)
        stream.Seek(int64 crcStart, SeekOrigin.Begin) |> ignore
        binaryWriter.Write(int32ToBigEndian crc)

        //write total size and command size
        stream.Seek(0L, SeekOrigin.Begin) |> ignore
        binaryWriter.Write(int32ToBigEndian totalSize)
        binaryWriter.Write(int32ToBigEndian commandSize)

        stream.Seek(0L, SeekOrigin.Begin) |> ignore                
        stream.CopyToAsync(output)

let newPartitionMetadataRequest(topicName : string) (requestId : RequestId) : SerializedPayload =
    let request = CommandPartitionedTopicMetadata(Topic = topicName, RequestId = %requestId)
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadata, partitionMetadata = request)
    serializeSimpleCommand command

let newSend (producerId : ProducerId) (sequenceId : SequenceId) (numMessages : int) (msgMetadata : MessageMetadata) (payload: byte[]) : SerializedPayload =    
    let request = CommandSend(ProducerId = %producerId, SequenceId = %sequenceId, NumMessages = numMessages)
    let command = BaseCommand(``type`` = CommandType.Send, Send = request)
    serializePayloadCommand command msgMetadata payload

let newAck (consumerId : ConsumerId) (ledgerId : LedgerId) (entryId : EntryId)
    (ackType : CommandAck.AckType) : SerializedPayload =
    Unchecked.defaultof<SerializedPayload>

let newConnect (clientVersion: string) (protocolVersion: ProtocolVersion) (proxyToBroker: Option<DnsEndPoint>) : SerializedPayload =    
    let request = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int) protocolVersion)
    match proxyToBroker with
    | Some logicalAddress -> request.ProxyToBrokerUrl <- sprintf "%s:%d" logicalAddress.Host logicalAddress.Port
    | None -> ()
    let command = BaseCommand(``type`` = CommandType.Connect, Connect = request)
    command |> serializeSimpleCommand

let newPong () : SerializedPayload =
    let request = CommandPong()
    let command = BaseCommand(``type`` = CommandType.Pong, Pong = request)
    command |> serializeSimpleCommand

let newLookup (topicName : string) (requestId : RequestId) (authoritative : bool) =
    let request = CommandLookupTopic(Topic = topicName, Authoritative = authoritative, RequestId = uint64(%requestId))
    let command = BaseCommand(``type`` = CommandType.Lookup, lookupTopic = request)
    command |> serializeSimpleCommand

let newProducer (topicName : string) (producerName: string) (producerId : ProducerId) (requestId : RequestId) =
    let request = CommandProducer(Topic = topicName, ProducerId = %producerId, RequestId = %requestId, ProducerName = producerName)
    let command = BaseCommand(``type`` = CommandType.Producer, Producer = request)
    command |> serializeSimpleCommand

let newGetTopicsOfNamespaceRequest (ns : NamespaceName) (requestId : RequestId) (mode : TopicDomain) =
    let mode =
        match mode with
        | Persistent -> CommandGetTopicsOfNamespace.Mode.Persistent
        | NonPersistent -> CommandGetTopicsOfNamespace.Mode.NonPersistent
    let request = CommandGetTopicsOfNamespace(Namespace = ns.ToString(), RequestId = uint64(%requestId), mode = mode)
    let command = BaseCommand(``type`` = CommandType.GetTopicsOfNamespace, getTopicsOfNamespace = request)
    command |> serializeSimpleCommand