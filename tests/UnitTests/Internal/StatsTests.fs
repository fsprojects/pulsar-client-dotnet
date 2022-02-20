module Pulsar.Client.UnitTests.Internal.Stats

open System.Threading.Tasks
open Expecto
open Expecto.Flip
open Pulsar.Client.Internal
open Pulsar.Client.Common
open System

[<Tests>]
let tests =

    testList "Stats" [

        testTask "Producer stats" {
            let stats = ProducerStatsImpl("unit_test") :> IProducerStatsRecorder
            let numberOfMessages = 10L
            let messageSize = 50L
            let latencyMs = 10.0

            
            stats.UpdateNumMsgsSent(int numberOfMessages, int numberOfMessages * (int messageSize))
            stats.IncrementSendFailed(int numberOfMessages)
            for _ in [1..int numberOfMessages] do
                stats.IncrementNumAcksReceived(TimeSpan.FromMilliseconds(latencyMs))
            
            let realBeforeTick = stats.GetStats()
            stats.TickTime(5)
            let realAfterTick = stats.GetStats()
            do! Task.Delay 10 // need to have a duration of large zero
            stats.TickTime(3)
            let realAfter2Tick = stats.GetStats()
            
            let expectedBeforeTick =
                {
                    NumMsgsSent = 0L
                    NumBytesSent = 0L
                    NumSendFailed = 0L
                    NumAcksReceived = 0L
                    SendMsgsRate = 0.0
                    SendBytesRate = 0.0
                    SendLatencyMin = Double.MaxValue
                    SendLatencyMax = Double.MinValue
                    SendLatencyAverage = 0.0
                    TotalMsgsSent = 0L
                    TotalBytesSent = 0L
                    TotalSendFailed = 0L
                    TotalAcksReceived = 0L
                    IntervalDuration = 0.0
                    PendingMsgs = 0
                }
            
            let expectedAfterTick =
                {
                    NumMsgsSent = numberOfMessages
                    NumBytesSent = numberOfMessages * messageSize
                    NumSendFailed = numberOfMessages
                    NumAcksReceived = numberOfMessages
                    SendMsgsRate = float numberOfMessages / realAfterTick.IntervalDuration * 1_000.0
                    SendBytesRate = float (numberOfMessages * messageSize) / realAfterTick.IntervalDuration * 1_000.0
                    SendLatencyMin = latencyMs
                    SendLatencyMax = latencyMs
                    SendLatencyAverage = latencyMs
                    TotalMsgsSent = numberOfMessages
                    TotalBytesSent = numberOfMessages * messageSize
                    TotalSendFailed = numberOfMessages
                    TotalAcksReceived = numberOfMessages
                    IntervalDuration = realAfterTick.IntervalDuration
                    PendingMsgs = 5
                }

            let expectedAfter2Tick =
                {
                    NumMsgsSent = 0L
                    NumBytesSent = 0L
                    NumSendFailed = 0L
                    NumAcksReceived = 0L
                    SendMsgsRate = 0.0
                    SendBytesRate = 0.0
                    SendLatencyMin = Double.MaxValue
                    SendLatencyMax = Double.MinValue
                    SendLatencyAverage = 0.0
                    TotalMsgsSent = numberOfMessages
                    TotalBytesSent = numberOfMessages * messageSize
                    TotalSendFailed = numberOfMessages
                    TotalAcksReceived = numberOfMessages
                    IntervalDuration = realAfter2Tick.IntervalDuration
                    PendingMsgs = 3
                }

            Expect.equal "" realBeforeTick expectedBeforeTick
            Expect.equal "" realAfterTick expectedAfterTick
            Expect.equal "" realAfter2Tick expectedAfter2Tick
        }
        
        testTask "Consumer stats" {
            let stats = ConsumerStatsImpl("unit_test") :> IConsumerStatsRecorder
            let numberOfMessages = 10L
            let messageSize = 50L
            
            stats.IncrementNumAcksSent(int numberOfMessages)
            for _ in [1..int numberOfMessages] do
                stats.UpdateNumMsgsReceived(int messageSize)
                stats.IncrementNumAcksFailed()
                stats.IncrementNumReceiveFailed()
                stats.IncrementNumBatchReceiveFailed()
            
            let realBeforeTick = stats.GetStats()
            stats.TickTime(5)
            let realAfterTick = stats.GetStats()
            do! Task.Delay 10 // need to have a duration of large zero
            stats.TickTime(3)
            let realAfter2Tick = stats.GetStats()

            let expectedBeforeTick =
                {
                    NumMsgsReceived = 0L
                    NumBytesReceived = 0L
                    NumReceiveFailed = 0L
                    NumBatchReceiveFailed = 0L
                    NumAcksSent = 0L
                    NumAcksFailed = 0L
                    
                    TotalMsgsReceived = 0L
                    TotalBytesReceived = 0L
                    TotalReceiveFailed = 0L
                    TotalBatchReceiveFailed = 0L
                    TotalAcksSent = 0L
                    TotalAcksFailed = 0L
                    
                    ReceivedMsgsRate = 0.0
                    ReceivedBytesRate = 0.0
                    IntervalDuration = 0.0
                    IncomingMsgs = 0
                }

            let expectedAfterTick =
                {
                    NumMsgsReceived = numberOfMessages
                    NumBytesReceived = numberOfMessages * messageSize
                    NumReceiveFailed = numberOfMessages
                    NumBatchReceiveFailed = numberOfMessages
                    NumAcksSent = numberOfMessages
                    NumAcksFailed = numberOfMessages
                    
                    TotalMsgsReceived = numberOfMessages
                    TotalBytesReceived = numberOfMessages * messageSize
                    TotalReceiveFailed = numberOfMessages
                    TotalBatchReceiveFailed = numberOfMessages
                    TotalAcksSent = numberOfMessages
                    TotalAcksFailed = numberOfMessages
                    
                    ReceivedMsgsRate = float numberOfMessages / realAfterTick.IntervalDuration * 1_000.0
                    ReceivedBytesRate = float (numberOfMessages * messageSize) / realAfterTick.IntervalDuration * 1_000.0
                    IntervalDuration = realAfterTick.IntervalDuration
                    IncomingMsgs = 5
                }

            let expectedAfter2Tick =
                {
                    NumMsgsReceived = 0L
                    NumBytesReceived = 0L
                    NumReceiveFailed = 0L
                    NumBatchReceiveFailed = 0L
                    NumAcksSent = 0L
                    NumAcksFailed = 0L
                    
                    TotalMsgsReceived = numberOfMessages
                    TotalBytesReceived = numberOfMessages * messageSize
                    TotalReceiveFailed = numberOfMessages
                    TotalBatchReceiveFailed = numberOfMessages
                    TotalAcksSent = numberOfMessages
                    TotalAcksFailed = numberOfMessages
                    
                    ReceivedMsgsRate = 0.0
                    ReceivedBytesRate = 0.0
                    IntervalDuration = realAfter2Tick.IntervalDuration
                    IncomingMsgs = 3
                }
            
            Expect.equal "" realBeforeTick expectedBeforeTick
            Expect.equal "" realAfterTick expectedAfterTick
            Expect.equal "" realAfter2Tick expectedAfter2Tick

        }
    ]