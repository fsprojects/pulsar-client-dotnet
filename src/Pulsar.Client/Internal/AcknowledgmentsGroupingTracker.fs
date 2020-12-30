namespace Pulsar.Client.Internal

open System.Collections
open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers
open FSharp.UMX

type internal GroupingTrackerMessage =
    | IsDuplicate of (MessageId*AsyncReplyChannel<bool>)
    | AddAcknowledgment of (MessageId*AckType*IReadOnlyDictionary<string, int64>*AsyncReplyChannel<unit>)
    | AddBatchIndexAcknowledgment of (MessageId*AckType*IReadOnlyDictionary<string, int64>*AsyncReplyChannel<unit>)
    | FlushAndClean
    | Flush
    | Stop

type internal LastCumulativeAck = MessageId

type internal IAcknowledgmentsGroupingTracker =
    abstract member IsDuplicate: MessageId -> bool
    abstract member AddAcknowledgment: MessageId * AckType * IReadOnlyDictionary<string, int64> -> Async<unit>
    abstract member AddBatchIndexAcknowledgment: MessageId * AckType * IReadOnlyDictionary<string, int64> -> Async<unit>
    abstract member FlushAndClean: unit -> unit
    abstract member Flush: unit -> unit
    abstract member Close: unit -> unit

type internal AcknowledgmentsGroupingTracker(prefix: string, consumerId: ConsumerId, ackGroupTime: TimeSpan,
                                    getState: unit -> ConnectionState,
                                    sendAckPayload: ClientCnx -> Payload -> Async<bool>) =

    [<Literal>]
    let MAX_ACK_GROUP_SIZE = 1000

    let pendingIndividualAcks = SortedSet<MessageId>()
    let pendingIndividualBatchIndexAcks = SortedSet<MessageId>()
    let mutable cumulativeAckFlushRequired = false
    let mutable lastCumulativeAck = MessageId.Earliest
    let mutable lastCumulativeAckIsBatch = false
    
    let getBatchDetails msgId =
        match msgId with
        | Individual -> failwith "Unexpected msgId type, expected Cumulative"
        | Batch x -> x

    let doCumulativeAck msgId isBatch =
        if msgId > lastCumulativeAck then
            lastCumulativeAck <- msgId
            lastCumulativeAckIsBatch <- isBatch
            cumulativeAckFlushRequired <- true
    
    let prefix = prefix + " GroupingTracker"

    let getAckData (msgId: MessageId) =
        let (_, acker) = getBatchDetails msgId.Type
        let ackSet = acker.BitSet |> toLongArray
        (msgId.LedgerId, msgId.EntryId, ackSet)
    
    let flush () =
        async {
            if not cumulativeAckFlushRequired && pendingIndividualAcks.Count = 0 && pendingIndividualBatchIndexAcks.Count = 0 then
                return ()
            else
                let! result =
                    async {
                        match getState() with
                        | Ready cnx ->
                            let mutable success = true
                            if cumulativeAckFlushRequired then
                                let ackSet =
                                    if lastCumulativeAckIsBatch then
                                        let (_, acker) = lastCumulativeAck.Type |> getBatchDetails
                                        acker.BitSet |> toLongArray
                                    else
                                        null
                                let payload = Commands.newAck consumerId lastCumulativeAck.LedgerId lastCumulativeAck.EntryId
                                                  AckType.Cumulative (Dictionary()) ackSet
                                let! ackSuccess = sendAckPayload cnx payload
                                if ackSuccess then
                                    cumulativeAckFlushRequired <- false
                                    Log.Logger.LogDebug("{0} newAck completed, acked {1} cumulatively", prefix, lastCumulativeAck)
                                success <- ackSuccess
                            let allMultiackMessages = ResizeArray()
                            if success && pendingIndividualAcks.Count > 0 then
                                let messages =
                                    seq {
                                        while pendingIndividualAcks.Count > 0 do
                                            let messageId = pendingIndividualAcks.Min
                                            pendingIndividualAcks.Remove(messageId) |> ignore
                                            // if messageId is chunked then all the chunked related to that msg also processed so, ack all of them
                                            match messageId.ChunkMessageIds with
                                            | Some messageIds ->
                                                for chunkedMessageId in messageIds do
                                                    yield (chunkedMessageId.LedgerId, chunkedMessageId.EntryId, null)
                                            | None ->
                                                yield (messageId.LedgerId, messageId.EntryId, null)
                                    }
                                allMultiackMessages.AddRange(messages)
                            if success && pendingIndividualBatchIndexAcks.Count > 0 then
                                let messages =
                                    seq {
                                        let mutable prevoiusMessageId = pendingIndividualBatchIndexAcks.Min
                                        pendingIndividualBatchIndexAcks.Remove(prevoiusMessageId) |> ignore
                                        while pendingIndividualBatchIndexAcks.Count > 0 do
                                            let messageId = pendingIndividualBatchIndexAcks.Min
                                            pendingIndividualBatchIndexAcks.Remove(messageId) |> ignore
                                            if (messageId.EntryId = prevoiusMessageId.EntryId &&
                                                messageId.LedgerId = prevoiusMessageId.LedgerId &&
                                                 messageId.Partition = prevoiusMessageId.Partition) then
                                                ()
                                            else
                                                yield getAckData prevoiusMessageId
                                            prevoiusMessageId <- messageId
                                        yield getAckData prevoiusMessageId
                                    }
                                allMultiackMessages.AddRange(messages)
                            if allMultiackMessages.Count > 0 then
                                let payload = Commands.newMultiMessageAck consumerId allMultiackMessages
                                let! ackSuccess = sendAckPayload cnx payload
                                if ackSuccess then
                                    Log.Logger.LogDebug("{0} newMultiMessageAck completed, acked {1} messages",
                                                        prefix, allMultiackMessages.Count)
                                success <- ackSuccess
                            return success
                        | _ ->
                            return false
                    }
                if not result then
                    Log.Logger.LogDebug("{0} Cannot flush pending acks since we're not connected to broker", prefix)
                return ()
        }

    let doImmediateAck (msgId: MessageId) ackType properties =
        async {
            let! result =
                async {
                    match getState() with
                    | Ready cnx ->
                        let payload = Commands.newAck consumerId msgId.LedgerId msgId.EntryId ackType properties null
                        return! sendAckPayload cnx payload
                    | _ ->
                        return false
                }
            if result then
                Log.Logger.LogDebug("{0} Successfully acked {1}", prefix, msgId)
            else
                Log.Logger.LogWarning("{0} Cannot doImmediateAck since we're not connected to broker", prefix)
            return ()
        }
    
    let doImmediateBatchIndexAck (msgId: MessageId) ackType properties =
        async {
            let! result =
                async {
                    match getState() with
                    | Ready cnx ->
                        let (index, acker) = getBatchDetails msgId.Type
                        let i = %index
                        let bitSet = BitArray(acker.GetBatchSize(), true)
                        let payload =
                                match ackType with
                                | AckType.Individual ->
                                    bitSet.[i] <- false
                                | AckType.Cumulative ->
                                    for j in 0 .. i do
                                        bitSet.[j] <- false
                                let ackSet = bitSet |> toLongArray
                                Commands.newAck consumerId msgId.LedgerId msgId.EntryId ackType properties ackSet
                        return! sendAckPayload cnx payload
                    | _ ->
                        return false
                }
            if not result then
                Log.Logger.LogWarning("{0} Cannot doImmediateAck since we're not connected to broker", prefix)
            return ()
        }
    
    
    let mb = MailboxProcessor<GroupingTrackerMessage>.Start(fun inbox ->
        let rec loop ()  =
            async {
                let! message = inbox.Receive()
                match message with
                | GroupingTrackerMessage.IsDuplicate (msgId, channel) ->
                    
                    if msgId <= lastCumulativeAck then
                        Log.Logger.LogDebug("{0} Message {1} already included in a cumulative ack", prefix, msgId)
                        channel.Reply(true)
                    else
                        channel.Reply(pendingIndividualAcks.Contains msgId)
                    return! loop ()
                    
                | GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, properties, channel) ->
                    
                    if ackGroupTime = TimeSpan.Zero || properties.Count > 0 then
                        // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
                        // uncommon condition since it's only used for the compaction subscription
                        do! doImmediateAck msgId ackType properties
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately acked", prefix, msgId)
                        channel.Reply()
                        return! loop ()
                    elif ackType = AckType.Cumulative then
                        // cumulative ack
                        doCumulativeAck msgId false
                        channel.Reply()
                        return! loop ()
                    else
                        // Individual ack
                        if pendingIndividualAcks.Add msgId then
                            if pendingIndividualAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush ()
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added to the ack tracker", prefix, msgId)
                        channel.Reply()
                        return! loop ()
                        
                | GroupingTrackerMessage.AddBatchIndexAcknowledgment (msgId, ackType, properties, channel) ->
                    
                    if ackGroupTime = TimeSpan.Zero || properties.Count > 0 then
                        do! doImmediateBatchIndexAck msgId ackType properties
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately batch index acked", prefix, msgId)
                        channel.Reply()
                        return! loop ()
                    elif ackType = AckType.Cumulative then
                        doCumulativeAck msgId true
                        channel.Reply()
                        return! loop ()
                    else
                        if pendingIndividualBatchIndexAcks.Add(msgId) then
                            if pendingIndividualBatchIndexAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush ()
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added", prefix, msgId)
                        channel.Reply()
                        return! loop ()
                
                        
                | GroupingTrackerMessage.FlushAndClean ->
                    
                    do! flush()
                    pendingIndividualAcks.Clear()
                    cumulativeAckFlushRequired <- false
                    lastCumulativeAck <- MessageId.Earliest
                    return! loop ()
                    
                | Flush ->
                    
                    do! flush()
                    return! loop ()
                    
                | Stop ->
                    
                    do! flush()
            }
        loop ()
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
        member this.AddAcknowledgment(msgId, ackType, properties) =
            mb.PostAndAsyncReply (fun channel -> GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, properties, channel))
        member this.AddBatchIndexAcknowledgment(msgId, ackType, properties) =
            mb.PostAndAsyncReply (fun channel -> GroupingTrackerMessage.AddBatchIndexAcknowledgment (msgId, ackType, properties, channel))
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
                member this.AddAcknowledgment(msgId, ackType, properties) = async { return () }
                member this.AddBatchIndexAcknowledgment(msgId, ackType, properties) = async { return () }
                member this.Flush() = ()
                member this.FlushAndClean() = ()
                member this.Close() = ()
        }

