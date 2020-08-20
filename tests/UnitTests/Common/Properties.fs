module Pulsar.Client.UnitTests.Common.Properties

open Expecto
open Pulsar.Client.Common
open FSharp.UMX
open FsCheck
open Pulsar.Client.Internal

type MessageIdGen() =
   static member MessageId() : Arbitrary<MessageId> =
       let createMessageId entryId ledgerId partition (batchIndex: int option) =
           match batchIndex with
           | Some index ->
               {
                   LedgerId = %(int64 ledgerId)
                   EntryId = %(int64 entryId)
                   Type = Cumulative (%index, BatchMessageAcker.NullAcker)
                   Partition = partition
                   TopicName = %""
                   ChunkMessageIds = None
               }
           | None ->
               {
                   LedgerId = %(int64 ledgerId)
                   EntryId = %(int64 entryId)
                   Type = Individual
                   Partition = partition
                   TopicName = %""
                   ChunkMessageIds = None
               }
       let getInt = Arb.generate<int> |> Gen.filter(fun i -> i >= -1)
       let batchIndex = Arb.generate<int> |> Gen.filter(fun i -> i >= 0) |> Gen.optionOf
       let genUser =
           createMessageId <!> getInt <*> getInt <*> getInt <*> batchIndex
       genUser |> Arb.fromGen


[<Tests>]
let tests =

    let config = { FsCheckConfig.defaultConfig with arbitrary = [typeof<MessageIdGen>] }

    testList "DTO" [
        testPropertyWithConfig config "MessageId serialization/deserialization works properly" <| fun (msgId: MessageId) ->
            let bytes = msgId.ToByteArray()
            let newMsgId = MessageId.FromByteArray(bytes)
            newMsgId = msgId
    ]