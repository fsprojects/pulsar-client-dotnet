module Pulsar.Client.UnitTests.Internal.AcknowledgmentsGroupingTrackerTests

open System
open System.IO
open System.IO.Pipelines
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open FSharp.UMX

let private writeSendTask ((write, _): SendTask) =
    use stream = new MemoryStream()
    let writer = PipeWriter.Create(stream, StreamPipeWriterOptions(leaveOpen = true))
    (write writer).GetAwaiter().GetResult()
    writer.Complete()
    stream.ToArray()

[<Tests>]
let tests =

    testList "AcknowledgmentsGroupingTracker" [

        testTask "Immediate ack is sent if ackGroupTime is zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                task {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.Zero, getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }, Individual, EmptyProperties)
            do! Task.Delay(45)
            Expect.isTrue "" sendPayloadCalled
        }

        testTask "Immediate ack is not sent if ackGroupTime is not zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                task {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(100.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }, Individual, EmptyProperties)
            do! Task.Delay(45)
            Expect.isFalse "" sendPayloadCalled
        }

        testTask "Ack is eventually sent if ackGroupTime is not zero" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalled = false
            let sendPayload cnx payload =
                task {
                    sendPayloadCalled <- true
                    return true
                }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment( { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }, Individual, EmptyProperties)
            do! Task.Delay(100)
            Expect.isTrue "" sendPayloadCalled
        }

        testTask "Cumulative ack works correctly" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                task {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment(message2, AckType.Cumulative, EmptyProperties)
            do! Task.Delay(100)
            Expect.equal "" 1 sendPayloadCalledCount
            let! isDuplicate = ackTracker.IsDuplicate message1
            Expect.isTrue "" isDuplicate
        }

        testTask "Multiple messages get multiacked" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                task {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message3 = { LedgerId = %1L; EntryId = %3L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment(message1, AckType.Cumulative, EmptyProperties)
            ackTracker.AddAcknowledgment(message2, AckType.Cumulative, EmptyProperties)
            ackTracker.AddAcknowledgment(message3, AckType.Cumulative, EmptyProperties)
            do! Task.Delay(100)
            Expect.equal "" 1 sendPayloadCalledCount
            let! isDuplicate = ackTracker.IsDuplicate message1
            Expect.isTrue "" isDuplicate
        }

        testTask "AddBatchIndexAcknowledgment works" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                task {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let acker = BatchMessageAcker(3)
            acker.AckIndividual(%0) |> ignore
            acker.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%0, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%1, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddBatchIndexAcknowledgment(message1, Individual, readOnlyDict [("1", 2L)])
            ackTracker.AddBatchIndexAcknowledgment(message2, Individual, EmptyProperties)

            do! Task.Delay(100)
            Expect.equal "" 2 sendPayloadCalledCount
        }

        testTask "MixedAcknowledgment works" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let mutable sendPayloadCalledCount = 0
            let sendPayload cnx payload =
                task {
                    sendPayloadCalledCount <- sendPayloadCalledCount + 1
                    return true
                }
            let acker1 = BatchMessageAcker(2)
            acker1.AckIndividual(%0) |> ignore
            acker1.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%0, acker1); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let acker2 = BatchMessageAcker(2)
            acker2.AckIndividual(%0) |> ignore
            acker2.AckIndividual(%1) |> ignore
            let message3 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Batch(%0, acker2); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message4 = { LedgerId = %1L; EntryId = %2L; Type = MessageIdType.Batch(%1, acker2); Partition = 0; TopicName = %""; ChunkMessageIds = None }

            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.FromMilliseconds(50.0), getState, sendPayload) :> IAcknowledgmentsGroupingTracker
            ackTracker.AddAcknowledgment(message1, Individual, EmptyProperties)
            ackTracker.AddBatchIndexAcknowledgment(message3, Individual, EmptyProperties)
            ackTracker.AddBatchIndexAcknowledgment(message4, Individual, EmptyProperties)

            do! Task.Delay(100)
            Expect.equal "" 1 sendPayloadCalledCount
        }

        testTask "Individual acks are retried after failed flush" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let payloads = ResizeArray<byte[]>()
            let sendPayload _cnx payload =
                task {
                    payloads.Add(writeSendTask payload)
                    return payloads.Count > 1
                }
            let messageId = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.MaxValue, getState, sendPayload) :> IAcknowledgmentsGroupingTracker

            ackTracker.AddAcknowledgment(messageId, Individual, EmptyProperties)
            do! ackTracker.FlushAsync(getState())
            do! ackTracker.FlushAsync(getState())

            Expect.equal "" 2 payloads.Count
            Expect.sequenceEqual "" payloads[0] payloads[1]

            ackTracker.Close()
        }

        testTask "Batch acks are retried after failed flush" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let payloads = ResizeArray<byte[]>()
            let sendPayload _cnx payload =
                task {
                    payloads.Add(writeSendTask payload)
                    return payloads.Count > 1
                }
            let acker = BatchMessageAcker(2)
            acker.AckIndividual(%0) |> ignore
            acker.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%0, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%1, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.MaxValue, getState, sendPayload) :> IAcknowledgmentsGroupingTracker

            ackTracker.AddBatchIndexAcknowledgment(message1, Individual, EmptyProperties)
            ackTracker.AddBatchIndexAcknowledgment(message2, Individual, EmptyProperties)
            do! ackTracker.FlushAsync(getState())
            do! ackTracker.FlushAsync(getState())

            Expect.equal "" 2 payloads.Count
            Expect.sequenceEqual "" payloads[0] payloads[1]

            ackTracker.Close()
        }

        testTask "Individual acks are not retried after successful flush" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let payloads = ResizeArray<byte[]>()
            let sendPayload _cnx payload =
                task {
                    payloads.Add(writeSendTask payload)
                    return true
                }
            let messageId = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Single; Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.MaxValue, getState, sendPayload) :> IAcknowledgmentsGroupingTracker

            ackTracker.AddAcknowledgment(messageId, Individual, EmptyProperties)
            do! ackTracker.FlushAsync(getState())
            do! ackTracker.FlushAsync(getState())

            Expect.equal "" 1 payloads.Count

            ackTracker.Close()
        }

        testTask "Batch acks are not retried after successful flush" {
            let getState() = ConnectionState.Ready Unchecked.defaultof<ClientCnx>
            let payloads = ResizeArray<byte[]>()
            let sendPayload _cnx payload =
                task {
                    payloads.Add(writeSendTask payload)
                    return true
                }
            let acker = BatchMessageAcker(2)
            acker.AckIndividual(%0) |> ignore
            acker.AckIndividual(%1) |> ignore
            let message1 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%0, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let message2 = { LedgerId = %1L; EntryId = %1L; Type = MessageIdType.Batch(%1, acker); Partition = 0; TopicName = %""; ChunkMessageIds = None }
            let ackTracker = AcknowledgmentsGroupingTracker("", %1UL, TimeSpan.MaxValue, getState, sendPayload) :> IAcknowledgmentsGroupingTracker

            ackTracker.AddBatchIndexAcknowledgment(message1, Individual, EmptyProperties)
            ackTracker.AddBatchIndexAcknowledgment(message2, Individual, EmptyProperties)
            do! ackTracker.FlushAsync(getState())
            do! ackTracker.FlushAsync(getState())

            Expect.equal "" 1 payloads.Count

            ackTracker.Close()
        }
    ]