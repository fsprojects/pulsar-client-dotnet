namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open Microsoft.Extensions.Logging
open System.Diagnostics

type internal IProducerStatsRecorder =
    abstract member UpdateNumMsgsSent: numMsgs:int * totalMsgsSize:int -> unit
    abstract member IncrementSendFailed: unit -> unit
    abstract member IncrementSendFailed: numMsgs:int -> unit
    abstract member IncrementNumAcksReceived: latencyNs:TimeSpan -> unit
    abstract member GetStats: unit -> ProducerStats
    abstract member TickTime: int -> unit

type ProducerStatsImpl(prefix: string) =
    let prefix = prefix + " ProducerStats"

    let mutable currentNumMsgsSent: int64 = 0L
    let mutable currentNumBytesSent: int64 = 0L
    let mutable currentNumSendFailed: int64 = 0L
    let mutable currentNumAcksReceived: int64 = 0L
    
    let mutable currentLatencySum: float = 0.0 
    let mutable currentSendLatencyMin: float = Double.MaxValue 
    let mutable currentSendLatencyMax: float = Double.MinValue
    
    let mutable numMsgsSent: int64 = 0L
    let mutable numBytesSent: int64 = 0L
    let mutable numSendFailed: int64 = 0L
    let mutable numAcksReceived: int64 = 0L
    let mutable sendMsgsRate: float = 0.0
    let mutable sendBytesRate: float = 0.0
    let mutable sendLatencyMin: float = Double.MaxValue
    let mutable sendLatencyMax: float = Double.MinValue
    let mutable sendLatencyAverage: float = 0.0
    let mutable totalMsgsSent: int64 = 0L
    let mutable totalBytesSent: int64 = 0L
    let mutable totalSendFailed: int64 = 0L
    let mutable totalAcksReceived: int64 = 0L
    let mutable intervalDuration: float = 0.0
    let mutable pendingMsgs: int = 0
    
    let sw = Stopwatch.StartNew()
    
    interface IProducerStatsRecorder with
        member this.UpdateNumMsgsSent(numMsgs, totalMsgsSize) =
            Log.Logger.LogDebug("{0} UpdateNumMsgsSent numMsgs:{1} totalMsgsSize:{2}", prefix, numMsgs, totalMsgsSize)
            currentNumMsgsSent <- currentNumMsgsSent + int64 numMsgs
            currentNumBytesSent <- currentNumBytesSent + int64 totalMsgsSize
        member this.IncrementSendFailed(numMsgs) =
            Log.Logger.LogDebug("{0} IncrementSendFailed numMsgs:{1}", prefix, numMsgs)
            currentNumSendFailed <- currentNumSendFailed + int64 numMsgs
        member this.IncrementSendFailed() =
            Log.Logger.LogDebug("{0} IncrementSendFailed numMsgs:{1}", prefix, 1)
            currentNumSendFailed <- currentNumSendFailed + 1L
        member this.IncrementNumAcksReceived(latency) =
            let latencyMs = latency.TotalMilliseconds
            Log.Logger.LogDebug("{0} IncrementNumAcksReceived latency:{1}", prefix, latencyMs)
            currentNumAcksReceived <- currentNumAcksReceived + 1L
            currentLatencySum <- currentLatencySum + latencyMs
            currentSendLatencyMax <- max latencyMs currentSendLatencyMax
            currentSendLatencyMin <- min latencyMs currentSendLatencyMin
        member this.GetStats() =
            Log.Logger.LogDebug("{0} GetStats", prefix)
            {
                NumMsgsSent = numMsgsSent
                NumBytesSent = numBytesSent
                NumSendFailed = numSendFailed
                NumAcksReceived = numAcksReceived
                SendMsgsRate = sendMsgsRate
                SendBytesRate = sendBytesRate
                SendLatencyMin = sendLatencyMin
                SendLatencyMax = sendLatencyMax
                SendLatencyAverage = sendLatencyAverage
                TotalMsgsSent = totalMsgsSent
                TotalBytesSent = totalBytesSent
                TotalSendFailed = totalSendFailed
                TotalAcksReceived = totalAcksReceived
                IntervalDuration = intervalDuration
                PendingMsgs = pendingMsgs
            }
            
        member this.TickTime pendingQueueSize =
            intervalDuration <- float sw.ElapsedMilliseconds
            sw.Restart()
            Log.Logger.LogDebug("{0} TickTime intervalDuration:{1}", prefix, intervalDuration)
            
            numMsgsSent <- currentNumMsgsSent
            numBytesSent <- currentNumBytesSent
            numSendFailed <- currentNumSendFailed
            numAcksReceived <- currentNumAcksReceived
            sendMsgsRate <- if intervalDuration > 0.0 then float currentNumMsgsSent / intervalDuration * 1_000.0 else 0.0
            sendBytesRate <- if intervalDuration > 0.0 then float currentNumBytesSent / intervalDuration * 1_000.0 else 0.0
            sendLatencyMin <- currentSendLatencyMin
            sendLatencyMax <- currentSendLatencyMax
            sendLatencyAverage <- if currentNumAcksReceived > 0L then currentLatencySum / float(currentNumAcksReceived) else 0.0
            totalMsgsSent <- totalMsgsSent + currentNumMsgsSent
            totalBytesSent <- totalBytesSent + currentNumBytesSent
            totalSendFailed <- totalSendFailed + currentNumSendFailed
            totalAcksReceived <- totalAcksReceived + currentNumAcksReceived
            pendingMsgs <- pendingQueueSize
            
            if currentNumMsgsSent > 0L || sendLatencyAverage > 0.0 || currentNumAcksReceived > 0L
               || currentNumSendFailed > 0L || pendingMsgs > 0 then
                let ackRate = float currentNumAcksReceived / intervalDuration * 1_000.0
                Log.Logger.LogInformation(
                    "{0} Pending messages: {1} --- Publish throughput: {2,1:0.00} msg/s --- {3,1:0.00} Mbit/s --- " + 
                    "Latency: min: {4} max: {5} average: {6} --- " +
                    "Ack received rate: {7,1:0.00} ack/s --- Failed messages: {8} --- Interval: {9,1:0.} ms",
                    prefix, pendingMsgs, sendMsgsRate, (sendBytesRate * 8.0 / 1024.0 / 1024.0),
                    sendLatencyMin, sendLatencyMax, sendLatencyAverage, ackRate, currentNumSendFailed, intervalDuration
                    )

            currentNumMsgsSent <- 0L
            currentNumBytesSent <- 0L
            currentNumSendFailed <- 0L
            currentNumAcksReceived <- 0L
            currentLatencySum <- 0.0
            currentSendLatencyMin <- Double.MaxValue 
            currentSendLatencyMax <- Double.MinValue


    static member internal PRODUCER_STATS_DISABLED = {
        new IProducerStatsRecorder with
            member this.UpdateNumMsgsSent(numMsgs, totalMsgsSize) = ()
            member this.IncrementSendFailed() = ()
            member this.IncrementSendFailed(numMsgs) = ()
            member this.IncrementNumAcksReceived(latency) = ()
            member this.GetStats() =
                {
                    NumMsgsSent = 0L
                    NumBytesSent = 0L
                    NumSendFailed = 0L
                    NumAcksReceived = 0L
                    SendMsgsRate = Double.NaN
                    SendBytesRate = Double.NaN
                    SendLatencyMin = Double.NaN
                    SendLatencyMax = Double.NaN
                    SendLatencyAverage = Double.NaN
                    TotalMsgsSent = 0L
                    TotalBytesSent = 0L
                    TotalSendFailed = 0L
                    TotalAcksReceived = 0L
                    IntervalDuration = Double.NaN
                    PendingMsgs = 0
                }
            member this.TickTime(pendingQueueSize) = ()
    }
