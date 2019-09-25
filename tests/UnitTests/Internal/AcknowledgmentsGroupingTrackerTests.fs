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
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }, AckType.Individual)
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
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }, AckType.Individual)
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
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(25.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }, AckType.Individual)
            do! Async.Sleep(50)
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
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(25.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment(message2, AckType.Cumulative)
            do! Async.Sleep(50)
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
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }
            let message3 = { LedgerId = %1L; EntryId = %3L; Type = MessageIdType.Individual; Partition = 0; TopicName = %"" }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(25.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            do! ackTracker.AddAcknowledgment(message1, AckType.Cumulative)
            do! ackTracker.AddAcknowledgment(message2, AckType.Cumulative)
            do! ackTracker.AddAcknowledgment(message3, AckType.Cumulative)
            do! Async.Sleep(50)
            Expect.equal "" 1 sendPayloadCalledCount
            let isDuplicate = ackTracker.IsDuplicate message1
            Expect.isTrue "" isDuplicate
        }
    ]