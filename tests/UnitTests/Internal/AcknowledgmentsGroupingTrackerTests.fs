module Pulsar.Client.UnitTests.Internal.AcknowledgmentsGroupingTrackerTests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open FSharp.UMX

[<Tests>]
let tests =

    testList "AcknowledgmentsGroupingTracker" [

        testAsync "Immediate ack is sent if ackGroupTime is zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                async {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.Zero, getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }, AckType.Individual, EmptyProperties)
            Expect.isTrue "" sendPayloadCalled
        }

        testAsync "Immediate ack is not sent if ackGroupTime is not zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                async {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(25.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }, AckType.Individual, EmptyProperties)
            Expect.isFalse "" sendPayloadCalled
        }

        testAsync "Ack is eventually sent if ackGroupTime is not zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                async {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }, AckType.Individual, EmptyProperties)
            do! Async.Sleep(100)
            Expect.isTrue "" sendPayloadCalled
        }

        testAsync "Cumulative ack works correctly" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                async {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment(message2, AckType.Cumulative, EmptyProperties)
            do! Async.Sleep(100)
            Expect.equal "" 1 sendPayloadCalledCount
            let isDuplicate = ackTracker.IsDuplicate message1
            Expect.isTrue "" isDuplicate
        }

        testAsync "Multiple messages get multiacked" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                async {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message3 = { LedgerId = %1L; EntryId = %3L; Type = MessageIdType.Individual; Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment(message1, AckType.Cumulative, EmptyProperties)
            do! ackTracker.AddAcknowledgment(message2, AckType.Cumulative, EmptyProperties)
            do! ackTracker.AddAcknowledgment(message3, AckType.Cumulative, EmptyProperties)
            do! Async.Sleep(100)
            Expect.equal "" 1 sendPayloadCalledCount
            let isDuplicate = ackTracker.IsDuplicate message1
            Expect.isTrue "" isDuplicate
        }
        
        testAsync "AddBatchIndexAcknowledgment works" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                async {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let acker = BatchMessageAcker(3)
            acker.AckIndividual(%0) |> ignore
            acker.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Cumulative(%0, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Cumulative(%1, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddBatchIndexAcknowledgment(message1, AckType.Individual, readOnlyDict [("1", 2L)])
            do! ackTracker.AddBatchIndexAcknowledgment(message2, AckType.Individual, EmptyProperties)
            
            do! Async.Sleep(100)
            Expect.equal "" 2 sendPayloadCalledCount
        }
        
        testAsync "MixedAcknowledgment works" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                async {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let acker1 = BatchMessageAcker(2)
            acker1.AckIndividual(%0) |> ignore
            acker1.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Cumulative(%0, acker1); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let acker2 = BatchMessageAcker(2)
            acker2.AckIndividual(%0) |> ignore
            acker2.AckIndividual(%1) |> ignore
            let message3 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Cumulative(%0, acker2); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message4 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Cumulative(%1, acker2); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment(message1, AckType.Individual, EmptyProperties)
            do! ackTracker.AddBatchIndexAcknowledgment(message3, AckType.Individual, EmptyProperties)
            do! ackTracker.AddBatchIndexAcknowledgment(message4, AckType.Individual, EmptyProperties)
            
            do! Async.Sleep(100)
            Expect.equal "" 1 sendPayloadCalledCount
        }
    ]