module Pulsar.Client.Common.Commands

open pulsar.proto
open System.IO
open ProtoBuf
open System.Buffers.Binary
open System.Net


let private serializeCommand instance =
    //get initial bytes
    use stream1 = new MemoryStream()
    Serializer.Serialize(stream1, instance)
    let commandBytes = stream1.ToArray()
    //calculate sizes
    let commandSize = commandBytes.Length
    let totalSize = commandSize + 4
    // serialize final array
    use stream2 = new MemoryStream()
    use writer = new BinaryWriter(stream2)
    writer.Write(IPAddress.HostToNetworkOrder(totalSize))
    writer.Write(IPAddress.HostToNetworkOrder(commandSize))
    writer.Write(commandBytes)
    writer.Flush()
    stream2.ToArray()

let newPartitionMetadataRequest (topicName: string) (requestId: RequestId) : byte[] =
    [||]

let newSend (producerId: ProducerId) (sequenceId: SequenceId) (numMessages: int) (checksumType: ChecksumType) (msgMetadata: MessageMetadata) payload : byte[] =
    [||]

let newAck (consumerId: ConsumerId) (ledgerId: LedgerId) (entryId: EntryId) (ackType: CommandAck.AckType) : byte[] =
    [||]

let newConnect (clientVersion: string) (protocolVersion: ProtocolVersion) : byte[] =
    let command = CommandConnect(ClientVersion = clientVersion, ProtocolVersion = (int)protocolVersion)
    let baseCommand = BaseCommand(``type`` = BaseCommand.Type.Connect, Connect = command)
    baseCommand |> serializeCommand
    