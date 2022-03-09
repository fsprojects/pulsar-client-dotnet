namespace Pulsar.Client.Common

open System
open System.IO
open FSharp.UMX
open ProtoBuf
open Pulsar.Client.Internal
open pulsar.proto

type BatchDetails = BatchIndex * BatchMessageAcker

type MessageIdType =
    | Single
    | Batch of BatchDetails

type AckSet = int64[]

[<CustomEquality; CustomComparison>]
type MessageId =
    {
        LedgerId: LedgerId
        EntryId: EntryId
        Type: MessageIdType
        Partition: int
        TopicName: CompleteTopicName
        ChunkMessageIds: MessageId[] option
    }
    with
        static member Earliest =
            {
                LedgerId = %(-1L)
                EntryId = %(-1L)
                Type = Single
                Partition = %(-1)
                TopicName = %""
                ChunkMessageIds = None
            }
        static member Latest =
            {
                LedgerId = %(Int64.MaxValue)
                EntryId = %(Int64.MaxValue)
                Type = Single
                Partition = %(-1)
                TopicName = %""
                ChunkMessageIds = None
            }
        member internal this.PrevBatchMessageId
            with get() = { this with EntryId = this.EntryId - %1L; Type = Single }
        member internal this.GetMessageIdData() =
            let data = MessageIdData(ledgerId = uint64 %this.LedgerId, entryId = uint64 %this.EntryId)
            if this.Partition >= 0 then
                data.Partition <- this.Partition
            match this.Type with
            | Batch (batchIndex, acker) when %batchIndex >= 0 ->
                data.BatchIndex <- %batchIndex
                if acker = BatchMessageAcker.NullAcker |> not then
                    data.BatchSize <- acker.GetBatchSize()
            | _ ->
                ()
            data
        member this.ToByteArray() =
            let data = this.GetMessageIdData()
            match this.ChunkMessageIds with
            | Some chunkMsgIds when chunkMsgIds.Length > 0 ->
                data.FirstChunkMessageId <- chunkMsgIds.[0].GetMessageIdData()
            | _ ->
                ()
            use stream = MemoryStreamManager.GetStream()
            Serializer.Serialize(stream, data)
            stream.ToArray()
        static member FromByteArray (data: byte[]) =
            use stream = new MemoryStream(data)
            let msgData = Serializer.Deserialize<MessageIdData>(stream)
            let msgType =
                if msgData.ShouldSerializeBatchIndex() then
                    let acker =
                        if msgData.ShouldSerializeBatchSize() then
                            BatchMessageAcker(msgData.BatchSize)
                        else
                            BatchMessageAcker.NullAcker
                    Batch (%msgData.BatchIndex, acker)
                else
                    Single
            let firstChunkIdData = msgData.FirstChunkMessageId
            let chunkMessageIds =
                if isNull firstChunkIdData then
                    None
                else
                    Some [| {
                        LedgerId = %(int64 firstChunkIdData.ledgerId)
                        EntryId = %(int64 firstChunkIdData.entryId)
                        Type = Single // The chunk message id must be single type
                        Partition = firstChunkIdData.Partition
                        TopicName = %""
                        ChunkMessageIds = None
                    } |]
            {
                LedgerId = %(int64 msgData.ledgerId)
                EntryId = %(int64 msgData.entryId)
                Type = msgType
                Partition = msgData.Partition
                TopicName = %""
                ChunkMessageIds = chunkMessageIds
            }
        static member FromByteArrayWithTopic (data: byte[], topicName: string) =
            let initial = MessageId.FromByteArray(data)
            { initial with TopicName = TopicName(topicName).CompleteTopicName }
        override this.ToString() =
            match this.Type with
            | Single ->
                sprintf "%d:%d:%d" this.LedgerId this.EntryId this.Partition
            | Batch (i, _) ->
                sprintf "%d:%d:%d:%d" this.LedgerId this.EntryId this.Partition i
        override this.Equals(other) =
            match other with
            | :? MessageId as m ->
                m.LedgerId = this.LedgerId && m.EntryId = this.EntryId && m.Partition = this.Partition &&
                    match m.Type, this.Type with
                    | (Single, Single) -> true
                    | (Batch (i, _), Batch (j, _)) -> i = j
                    | (Single, Batch (i, _)) -> i = %(-1)
                    | (Batch (i, _), Single) -> i = %(-1) 
                    &&
                    match m.ChunkMessageIds, this.ChunkMessageIds with
                    | Some mchunkMessageIds, Some thisChunkMessageIds when mchunkMessageIds.Length > 0 && thisChunkMessageIds.Length > 0 ->
                        mchunkMessageIds.[0] = thisChunkMessageIds.[0] // We need to check the first chunk message id if the message is a chunkd message
                    | _, _ -> true
            | _ ->
                false
            
        override this.GetHashCode() =
            match this.Type with
            | Single ->
                (31 * ((int this.LedgerId) + 31 * (int this.EntryId)) + this.Partition)
            | Batch (batchIndex, _) ->
                (31 * ((int this.LedgerId) + 31 * (int this.EntryId)) + (31 * this.Partition) + %batchIndex)
                
        interface IComparable<MessageId> with
            member this.CompareTo(other) =
                if this.LedgerId > other.LedgerId then
                    1
                elif this.LedgerId < other.LedgerId then
                    -1
                else
                    if this.EntryId > other.EntryId then
                        1
                    elif this.EntryId < other.EntryId then
                        -1
                    else
                        let typeComparison =
                            match this.Type, other.Type with
                            | Single, Single ->
                                0
                            | Batch (i, _), Batch (j, _) ->
                                if i > j then 1 elif j > i then -1 else 0
                            | Single, Batch (i, _) ->
                                if i > %(-1) then -2 else 0
                            | Batch (i, _), Single ->
                                if i > %(-1) then 2 else 0
                        match typeComparison with
                        | 0 -> compare this.Partition other.Partition
                        | 2 | -2 ->
                            match compare this.Partition other.Partition with
                            | 0 -> typeComparison
                            | partitionComparison -> partitionComparison
                        | _ ->
                            typeComparison
                    
        interface IComparable with
            member this.CompareTo(other) =
                match other with
                | :? MessageId as m ->
                    (this :> IComparable<MessageId>).CompareTo(m)
                | _ ->
                    failwith <| "Can't compare MessageId with another type: " + other.GetType().FullName

