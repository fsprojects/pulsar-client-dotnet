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

type LastAcknowledgeMessage = MessageId

type IAcknowledgmentsGroupingTracker =
    abstract member IsDuplicate: MessageId -> bool
    abstract member AddAcknowledgment: MessageId * AckType -> Async<unit>
    abstract member FlushAndClean: unit -> unit
    abstract member Flush: unit -> unit
    abstract member Close: unit -> unit

type AcknowledgmentsGroupingTracker(prefix: string, consumerId: ConsumerId, ackGroupTime: TimeSpan, handler: ConnectionHandler) =

    [<Literal>]
    let MAX_ACK_GROUP_SIZE = 1000

    let pendingIndividualAcks = SortedSet<MessageId>()
    let mutable cumulativeAckFlushRequired = false

    let prefix = prefix + " GroupingTracker"

    let flush (lastAcknowledgedMessage: LastAcknowledgeMessage) =
        async {
            if not cumulativeAckFlushRequired && pendingIndividualAcks.Count = 0 then
                return ()
            else
                let! result =
                    async {
                        match handler.ConnectionState with
                        | Ready cnx ->
                            let mutable success = true
                            if cumulativeAckFlushRequired then
                                let payload = Commands.newAck consumerId lastAcknowledgedMessage AckType.Cumulative
                                let! ackSuccess = cnx.Send payload
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
                                let! ackSuccess = cnx.Send payload
                                if ackSuccess then
                                    Log.Logger.LogDebug("{0} newMultiMessageAck completed", prefix)
                                success <- ackSuccess
                            return success
                        | _ ->
                            return false
                    }
                if not result then
                    Log.Logger.LogWarning("{0} Cannot flush pending acks since we're not connected to broker", prefix)
                return ()
        }

    let doImmediateAck msgId ackType =
        async {
            let! result =
                async {
                    match handler.ConnectionState with
                    | Ready cnx ->
                        let payload = Commands.newAck consumerId msgId ackType
                        return! cnx.Send payload
                    | _ ->
                        return false
                }
            if not result then
                Log.Logger.LogWarning("{0} Cannot doImmediateAck since we're not connected to broker", prefix)
            return ()
        }

    let mb = MailboxProcessor<GroupingTrackerMessage>.Start(fun inbox ->
        let rec loop (lastAcknowledgeMessage: LastAcknowledgeMessage)  =
            async {
                let! message = inbox.Receive()
                match message with
                | GroupingTrackerMessage.IsDuplicate (msgId, channel) ->
                    if msgId <= lastAcknowledgeMessage then
                        Log.Logger.LogDebug("{0} Message {1} already included in a cumulative ack", prefix, msgId)
                        channel.Reply(true)
                    else
                        channel.Reply(pendingIndividualAcks.Contains msgId)
                    return! loop lastAcknowledgeMessage
                | GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, channel) ->
                    if ackGroupTime = TimeSpan.Zero then
                        // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
                        // uncommon condition since it's only used for the compaction subscription
                        do! doImmediateAck msgId ackType
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately acked", prefix, msgId)
                        channel.Reply()
                        return! loop lastAcknowledgeMessage
                    elif ackType = AckType.Cumulative then
                        // cumulative ack
                        channel.Reply()
                        return! loop msgId
                    else
                        // Individual ack
                        if pendingIndividualAcks.Add(msgId) then
                            if pendingIndividualAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush lastAcknowledgeMessage
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added", prefix, msgId)
                        channel.Reply()
                        return! loop lastAcknowledgeMessage

                | GroupingTrackerMessage.FlushAndClean ->
                    do! flush lastAcknowledgeMessage
                    pendingIndividualAcks.Clear()
                    return! loop MessageId.Earliest
                | Flush ->
                    do! flush lastAcknowledgeMessage
                    return! loop lastAcknowledgeMessage
                | Stop ->
                    pendingIndividualAcks.Clear()
            }
        loop MessageId.Earliest
    )

    let timer = new Timer(ackGroupTime.TotalMilliseconds)
    do timer.AutoReset <- true
    do timer.Elapsed.Add(fun _ -> mb.Post Flush)
    do timer.Start()

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

