namespace Pulsar.Client.Internal

open System.Threading
open Pulsar.Client.Common
open System
open Microsoft.Extensions.Logging
open System.Diagnostics

type internal IConsumerStatsRecorder =
    abstract member UpdateNumMsgsReceived: msgSize:int -> unit
    abstract member IncrementNumAcksSent: numAcks:int -> unit
    abstract member IncrementNumAcksFailed: unit -> unit
    abstract member IncrementNumReceiveFailed: unit -> unit
    abstract member IncrementNumBatchReceiveFailed: unit -> unit
    abstract member GetStats: unit -> ConsumerStats
    abstract member TickTime: int -> unit

type ConsumerStatsImpl(prefix: string) =
    let prefix = prefix + " ConsumerStats"

    let mutable currentNumMsgsReceived: int64 = 0L
    let mutable currentNumBytesReceived: int64 = 0L
    let mutable currentNumAcksSent: int64 = 0L
    let mutable currentNumAcksFailed: int64 = 0L
    
    let mutable currentNumReceiveFailed: int64 = 0L 
    let mutable currentNumBatchReceiveFailed: int64 = 0L
    
    let mutable numMsgsReceived: int64 = 0L
    let mutable numBytesReceived: int64 = 0L
    let mutable numReceiveFailed: int64 = 0L
    let mutable numBatchReceiveFailed: int64 = 0L
    let mutable numAcksSent: int64 = 0L
    let mutable numAcksFailed: int64 = 0L
    
    let mutable totalMsgsReceived: int64 = 0L
    let mutable totalBytesReceived: int64 = 0L
    let mutable totalReceiveFailed: int64 = 0L
    let mutable totalBatchReceiveFailed: int64 = 0L
    let mutable totalAcksSent: int64 = 0L
    let mutable totalAcksFailed: int64 = 0L
    
    let mutable receivedMsgsRate: float = 0.0
    let mutable receivedBytesRate: float = 0.0
    let mutable intervalDuration: float = 0.0
    let mutable incomingMsgs: int = 0
    
    let sw = Stopwatch.StartNew()
    
    interface IConsumerStatsRecorder with
        member this.UpdateNumMsgsReceived msgSize =
            Log.Logger.LogDebug("{0} UpdateNumMsgsReceived msgSize:{1}", prefix, msgSize)
            currentNumMsgsReceived <- currentNumMsgsReceived + 1L
            currentNumBytesReceived <- currentNumBytesReceived + int64 msgSize
        member this.IncrementNumAcksSent numAcks =
            Log.Logger.LogDebug("{0} IncrementNumAcksSent numAcks:{1}", prefix, numAcks)
            currentNumAcksSent <- currentNumAcksSent + int64 numAcks
        member this.IncrementNumAcksFailed() =
            Log.Logger.LogDebug("{0} IncrementNumAcksFailed", prefix)
            Interlocked.Increment(&currentNumAcksFailed) |> ignore
        member this.IncrementNumReceiveFailed() =
            Log.Logger.LogDebug("{0} IncrementNumReceiveFailed", prefix)
            Interlocked.Increment(&currentNumReceiveFailed) |> ignore
        member this.IncrementNumBatchReceiveFailed() =
            Log.Logger.LogDebug("{0} IncrementNumBatchReceiveFailed", prefix)
            Interlocked.Increment(&currentNumBatchReceiveFailed) |> ignore
        member this.GetStats() =
            Log.Logger.LogDebug("{0} GetStats", prefix)
            {
                NumMsgsReceived = numMsgsReceived
                NumBytesReceived = numBytesReceived
                NumReceiveFailed = numReceiveFailed
                NumBatchReceiveFailed = numBatchReceiveFailed
                NumAcksSent = numAcksSent
                NumAcksFailed = numAcksFailed
                
                TotalMsgsReceived = totalMsgsReceived
                TotalBytesReceived = totalBytesReceived
                TotalReceiveFailed = totalReceiveFailed
                TotalBatchReceiveFailed = totalBatchReceiveFailed
                TotalAcksSent = totalAcksSent
                TotalAcksFailed = totalAcksFailed
                
                ReceivedMsgsRate = receivedMsgsRate
                ReceivedBytesRate = receivedBytesRate
                IntervalDuration = intervalDuration
                IncomingMsgs = incomingMsgs
            }
        
        member this.TickTime(incomingMessages) =
            intervalDuration <- float sw.ElapsedMilliseconds
            sw.Restart()
            Log.Logger.LogDebug("{0} TickTime intervalDuration:{1}", prefix, intervalDuration)
            
            numMsgsReceived <- currentNumMsgsReceived
            numBytesReceived <- currentNumBytesReceived
            numReceiveFailed <- Interlocked.Exchange(&currentNumReceiveFailed, 0L)
            numBatchReceiveFailed <- Interlocked.Exchange(&currentNumBatchReceiveFailed, 0L) 
            numAcksSent <- currentNumAcksSent
            numAcksFailed <- Interlocked.Exchange(&currentNumAcksFailed, 0L) 
            
            currentNumMsgsReceived <- 0L
            currentNumBytesReceived <- 0L
            currentNumAcksSent <- 0L
            
            totalMsgsReceived <- totalMsgsReceived + numMsgsReceived
            totalBytesReceived <- totalBytesReceived + numBytesReceived
            totalReceiveFailed <- totalReceiveFailed + numReceiveFailed
            totalBatchReceiveFailed <- totalBatchReceiveFailed + numBatchReceiveFailed
            totalAcksSent <- totalAcksSent + numAcksSent
            totalAcksFailed <- totalAcksFailed + numAcksFailed

            receivedMsgsRate <- if intervalDuration > 0.0 then  float numMsgsReceived / intervalDuration * 1_000.0 else 0.0
            receivedBytesRate <- if intervalDuration > 0.0 then float numBytesReceived / intervalDuration * 1_000.0 else 0.0
            incomingMsgs <- incomingMessages
            
            if numMsgsReceived > 0L || numBytesReceived > 0L || numReceiveFailed > 0L
               || numAcksSent > 0L || numAcksFailed > 0L then
                let ackRate = float numAcksSent / intervalDuration * 1_000.0
                Log.Logger.LogInformation(
                    "{0} Prefetched messages: {1} --- Consume throughput received: {2,1:0.00} msg/s --- {3,1:0.00} Mbit/s --- " + 
                    "Ack sent rate: {4,1:0.00} ack/s --- Failed messages: {5} --- batch messages: {6} --- Failed acks: {7} --- " +
                    "Interval: {8,1:0.} ms",
                    prefix, incomingMsgs, receivedMsgsRate, (receivedBytesRate * 8.0 / 1024.0 / 1024.0),
                    ackRate, numReceiveFailed, numBatchReceiveFailed, numAcksFailed, intervalDuration
                    )

    static member internal CONSUMER_STATS_DISABLED = {
        new IConsumerStatsRecorder with
            member this.UpdateNumMsgsReceived(_) = ()
            member this.IncrementNumAcksSent(_) = ()
            member this.IncrementNumAcksFailed() = ()
            member this.IncrementNumReceiveFailed() = ()
            member this.IncrementNumBatchReceiveFailed() = ()
            member this.GetStats() =
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
                    
                    ReceivedMsgsRate = Double.NaN
                    ReceivedBytesRate = Double.NaN
                    IntervalDuration = Double.NaN
                    IncomingMsgs = 0
                }
            member this.TickTime(_) = ()
    }
