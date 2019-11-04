namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers
open FSharp.UMX
open pulsar.proto

type GroupingTrackerMessage =
    | IsDuplicate of (MessageId*AsyncReplyChannel<bool>)
    | AddAcknowledgment of (MessageId*AckType*AsyncReplyChannel<unit>)
    | FlushAndClean
    | Flush
    | Stop

type LastCumulativeAck = MessageId

type IAcknowledgmentsGroupingTracker =
    abstract member IsDuplicate: MessageId -> bool
    abstract member AddAcknowledgment: MessageId * AckType -> Async<unit>
    abstract member FlushAndClean: unit -> unit
    abstract member Flush: unit -> unit
    abstract member Close: unit -> unit

type AcknowledgmentsGroupingTracker(prefix: string, consumerId: ConsumerId, ackGroupTime: TimeSpan,
                                    getState: unit -> ConnectionState,
                                    sendAckPayload: ClientCnx -> Payload -> Async<bool>) =

    [<Literal>]
    let MAX_ACK_GROUP_SIZE = 1000

    let pendingIndividualAcks = SortedSet<MessageId>()
    let mutable cumulativeAckFlushRequired = false

    let prefix = prefix + " GroupingTracker"

    let flush (lastCumulativeAck: LastCumulativeAck) =
        async {
            if not cumulativeAckFlushRequired && pendingIndividualAcks.Count = 0 then
                return ()
            else
                let! result =
                    async {
                        match getState() with
                        | Ready cnx ->
                            let mutable success = true
                            if cumulativeAckFlushRequired then
                                let payload = Commands.newAck consumerId lastCumulativeAck AckType.Cumulative
                                let! ackSuccess = sendAckPayload cnx payload
                                if ackSuccess then
                                    cumulativeAckFlushRequired <- false
                                    Log.Logger.LogDebug("{0} cumulativeAckFlush was required newAck completed", prefix)
                                success <- ackSuccess
                            if success && pendingIndividualAcks.Count > 0 then
                                let messages =
                                    seq {
                                        while pendingIndividualAcks.Count > 0 do
                                            let message = pendingIndividualAcks.Min
                                            pendingIndividualAcks.Remove(message) |> ignore
                                            yield message
                                    }
                                let payload = Commands.newMultiMessageAck consumerId messages
                                let! ackSuccess = sendAckPayload cnx payload
                                if ackSuccess then
                                    Log.Logger.LogDebug("{0} newMultiMessageAck completed", prefix)
                                success <- ackSuccess
                            return success
                        | _ ->
                            return false
                    }
                if not result then
                    Log.Logger.LogDebug("{0} Cannot flush pending acks since we're not connected to broker", prefix)
                return ()
        }

    let doImmediateAck msgId ackType =
        async {
            let! result =
                async {
                    match getState() with
                    | Ready cnx ->
                        let payload = Commands.newAck consumerId msgId ackType
                        return! sendAckPayload cnx payload
                    | _ ->
                        return false
                }
            if not result then
                Log.Logger.LogWarning("{0} Cannot doImmediateAck since we're not connected to broker", prefix)
            return ()
        }

    let mb = MailboxProcessor<GroupingTrackerMessage>.Start(fun inbox ->
        let rec loop (lastCumulativeAck: LastCumulativeAck)  =
            async {
                let! message = inbox.Receive()
                match message with
                | GroupingTrackerMessage.IsDuplicate (msgId, channel) ->
                    if msgId <= lastCumulativeAck then
                        Log.Logger.LogDebug("{0} Message {1} already included in a cumulative ack", prefix, msgId)
                        channel.Reply(true)
                    else
                        channel.Reply(pendingIndividualAcks.Contains msgId)
                    return! loop lastCumulativeAck
                | GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, channel) ->
                    if ackGroupTime = TimeSpan.Zero then
                        // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
                        // uncommon condition since it's only used for the compaction subscription
                        do! doImmediateAck msgId ackType
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately acked", prefix, msgId)
                        channel.Reply()
                        return! loop lastCumulativeAck
                    elif ackType = AckType.Cumulative then
                        // cumulative ack
                        cumulativeAckFlushRequired <- true
                        channel.Reply()
                        return! loop msgId
                    else
                        // Individual ack
                        if pendingIndividualAcks.Add(msgId) then
                            if pendingIndividualAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush lastCumulativeAck
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added", prefix, msgId)
                        channel.Reply()
                        return! loop lastCumulativeAck

                | GroupingTrackerMessage.FlushAndClean ->
                    do! flush lastCumulativeAck
                    pendingIndividualAcks.Clear()
                    return! loop MessageId.Earliest
                | Flush ->
                    do! flush lastCumulativeAck
                    return! loop lastCumulativeAck
                | Stop ->
                    do! flush lastCumulativeAck
            }
        loop MessageId.Earliest
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    let timer = new Timer()
    let tryLaunchTimer() =
        if ackGroupTime <> TimeSpan.Zero then
            timer.Interval <- ackGroupTime.TotalMilliseconds
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> mb.Post Flush)
            timer.Start()
    do tryLaunchTimer()

    interface IAcknowledgmentsGroupingTracker with
        /// Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
        /// resent after a disconnection and for which the user has already sent an acknowledgement.
        member this.IsDuplicate(msgId) =
            mb.PostAndReply (fun channel -> GroupingTrackerMessage.IsDuplicate (msgId, channel))
        member this.AddAcknowledgment(msgId, ackType) =
            mb.PostAndAsyncReply (fun channel -> GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, channel))
        member this.Flush() =
            mb.Post GroupingTrackerMessage.Flush
        member this.FlushAndClean() =
            mb.Post GroupingTrackerMessage.FlushAndClean
        member this.Close() =
            timer.Stop()
            mb.Post GroupingTrackerMessage.Stop


    static member NonPersistentAcknowledgmentGroupingTracker =
        {
            new IAcknowledgmentsGroupingTracker with
                member this.IsDuplicate(msgId) = false
                member this.AddAcknowledgment(msgId, ackType) = async { return () }
                member this.Flush() = ()
                member this.FlushAndClean() = ()
                member this.Close() = ()
        }

