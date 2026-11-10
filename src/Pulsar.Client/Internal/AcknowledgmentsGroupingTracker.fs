namespace Pulsar.Client.Internal

open System.Collections
open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers
open FSharp.UMX
open System.Threading.Tasks
open System.Threading.Channels


type internal GroupingTrackerMessage =
    | IsDuplicate of (MessageId*TaskCompletionSource<bool>)
    | AddAcknowledgment of (MessageId*AckType*IReadOnlyDictionary<string, int64>)
    | AddBatchIndexAcknowledgment of (MessageId*AckType*IReadOnlyDictionary<string, int64>)
    | FlushAndClean
    | Flush
    | FlushAsync of (ConnectionState*TaskCompletionSource<Unit>)
    | Stop

type internal LastCumulativeAck = MessageId

type internal IAcknowledgmentsGroupingTracker =
    abstract member IsDuplicate: MessageId -> Task<bool>
    abstract member AddAcknowledgment: MessageId * AckType * IReadOnlyDictionary<string, int64> -> unit
    abstract member AddBatchIndexAcknowledgment: MessageId * AckType * IReadOnlyDictionary<string, int64> -> unit
    abstract member FlushAndClean: unit -> unit
    abstract member Flush: unit -> unit
    abstract member FlushAsync: ConnectionState -> Task<unit>
    abstract member Close: unit -> unit

type internal AcknowledgmentsGroupingTracker(prefix: string, consumerId: ConsumerId, ackGroupTime: TimeSpan,
                                    getState: unit -> ConnectionState,
                                    sendAckPayload: ClientCnx -> SendTask -> Task<bool>) =

    [<Literal>]
    let MAX_ACK_GROUP_SIZE = 1000

    let pendingIndividualAcks = SortedSet<MessageId>()
    let pendingIndividualBatchIndexAcks = SortedSet<MessageId>()
    let mutable cumulativeAckFlushRequired = false
    let mutable lastCumulativeAck = MessageId.Earliest
    let mutable lastCumulativeAckIsBatch = false

    let getBatchDetails msgId =
        match msgId with
        | MessageIdType.Single -> failwith "Unexpected msgId type, expected Cumulative"
        | Batch x -> x

    let doCumulativeAck msgId isBatch =
        if msgId > lastCumulativeAck then
            lastCumulativeAck <- msgId
            lastCumulativeAckIsBatch <- isBatch
            cumulativeAckFlushRequired <- true

    let prefix = prefix + " GroupingTracker"

    let getAckData (msgId: MessageId) =
        let struct(_, acker) = getBatchDetails msgId.Type
        let ackSet = acker.BitSet |> toLongArray
        (msgId.LedgerId, msgId.EntryId, ackSet)

    let flush (clientCnxOption: ConnectionState option) =
        backgroundTask {
            if not cumulativeAckFlushRequired && pendingIndividualAcks.Count = 0 && pendingIndividualBatchIndexAcks.Count = 0 then
                return ()
            else
                let! result =
                    backgroundTask {
                        let state = clientCnxOption |> Option.defaultWith getState
                        match state with
                        | Ready cnx ->
                            let mutable success = true
                            if cumulativeAckFlushRequired then
                                let ackSet =
                                    if lastCumulativeAckIsBatch then
                                        let struct(_, acker) = lastCumulativeAck.Type |> getBatchDetails
                                        acker.BitSet |> toLongArray
                                    else
                                        null
                                let payload = Commands.newAck consumerId lastCumulativeAck.LedgerId lastCumulativeAck.EntryId
                                                  AckType.Cumulative EmptyProperties ackSet None None None None
                                let! ackSuccess = sendAckPayload cnx payload
                                if ackSuccess then
                                    cumulativeAckFlushRequired <- false
                                    Log.Logger.LogDebug("{0} newAck completed, acked {1} cumulatively", prefix, lastCumulativeAck)
                                success <- ackSuccess
                            let allMultiackMessages = ResizeArray()
                            if success && pendingIndividualAcks.Count > 0 then
                                let messages =
                                    seq {
                                        for messageId in pendingIndividualAcks do
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
                                        for messageId in pendingIndividualBatchIndexAcks do
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
                                    // Only drop pending ids after a successful send so a failed flush can retry.
                                    pendingIndividualAcks.Clear()
                                    pendingIndividualBatchIndexAcks.Clear()
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
        backgroundTask {
            let! result =
                backgroundTask {
                    match getState() with
                    | Ready cnx ->
                        let payload = Commands.newAck consumerId msgId.LedgerId msgId.EntryId ackType properties null
                                        None None None None
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
        backgroundTask {
            let! result =
                backgroundTask {
                    match getState() with
                    | Ready cnx ->
                        let struct(index, acker) = getBatchDetails msgId.Type
                        let i = %index
                        let bitSet = BitArray(acker.GetBatchSize(), true)
                        let payload =
                                match ackType with
                                | Individual ->
                                    bitSet[i] <- false
                                | AckType.Cumulative ->
                                    for j in 0 .. i do
                                        bitSet[j] <- false
                                let ackSet = bitSet |> toLongArray
                                Commands.newAck consumerId msgId.LedgerId msgId.EntryId ackType properties ackSet
                                    None None None None
                        return! sendAckPayload cnx payload
                    | _ ->
                        return false
                }
            if not result then
                Log.Logger.LogWarning("{0} Cannot doImmediateAck since we're not connected to broker", prefix)
            return ()
        }

    let mb = Channel.CreateUnbounded<GroupingTrackerMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    // why the loop stopped, if it failed; set before the mailbox is completed
    let mutable stopCause: exn = null

    let failRequest (ex: exn) (msg: GroupingTrackerMessage) =
        match msg with
        | GroupingTrackerMessage.IsDuplicate (_, channel) ->
            channel.TrySetException ex |> ignore
        | FlushAsync (_, channel) ->
            channel.TrySetException ex |> ignore
        | _ ->
            ()

    // The consumer's mailbox waits for these replies, so they are given even when the loop no longer runs. A closed
    // tracker has no acknowledgments: no message is a duplicate, and there is nothing to flush.
    let replyAfterStop (msg: GroupingTrackerMessage) =
        match System.Threading.Volatile.Read(&stopCause) with
        | null ->
            match msg with
            | GroupingTrackerMessage.IsDuplicate (_, channel) ->
                channel.TrySetResult false |> ignore
            | FlushAsync (_, channel) ->
                channel.TrySetResult() |> ignore
            | _ ->
                ()
        | ex ->
            failRequest ex msg

    let postAndReply (msg: GroupingTrackerMessage) =
        if not (mb.Writer.TryWrite msg) then
            replyAfterStop msg

    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            try
                match msg with
                | GroupingTrackerMessage.IsDuplicate (msgId, channel) ->

                    if msgId <= lastCumulativeAck then
                        Log.Logger.LogDebug("{0} Message {1} already included in a cumulative ack", prefix, msgId)
                        channel.SetResult(true)
                    else
                        channel.SetResult(pendingIndividualAcks.Contains msgId)

                | GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, properties) ->

                    if ackGroupTime = TimeSpan.Zero || properties.Count > 0 then
                        // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
                        // uncommon condition since it's only used for the compaction subscription
                        do! doImmediateAck msgId ackType properties
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately acked", prefix, msgId)
                    elif ackType = AckType.Cumulative then
                        // cumulative ack
                        doCumulativeAck msgId false
                    else
                        // Individual ack
                        if pendingIndividualAcks.Add msgId then
                            if pendingIndividualAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush None
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added to the ack tracker", prefix, msgId)

                | GroupingTrackerMessage.AddBatchIndexAcknowledgment (msgId, ackType, properties) ->

                    if ackGroupTime = TimeSpan.Zero || properties.Count > 0 then
                        do! doImmediateBatchIndexAck msgId ackType properties
                        Log.Logger.LogDebug("{0} messageId {1} has been immediately batch index acked", prefix, msgId)
                    elif ackType = AckType.Cumulative then
                        doCumulativeAck msgId true
                    else
                        if pendingIndividualBatchIndexAcks.Add(msgId) then
                            if pendingIndividualBatchIndexAcks.Count >= MAX_ACK_GROUP_SIZE then
                                do! flush None
                                Log.Logger.LogWarning("{0} messageId {1} MAX_ACK_GROUP_SIZE reached and flushed", prefix, msgId)
                        else
                            Log.Logger.LogWarning("{0} messageId {1} has already been added", prefix, msgId)

                | GroupingTrackerMessage.FlushAndClean ->

                    do! flush None
                    pendingIndividualAcks.Clear()
                    pendingIndividualBatchIndexAcks.Clear()
                    cumulativeAckFlushRequired <- false
                    lastCumulativeAck <- MessageId.Earliest

                | Flush ->

                    do! flush None

                | FlushAsync (connectionState, channel) ->

                    do! flush <| Some connectionState
                    channel.SetResult()

                | Stop ->

                    continueLoop <- false

            // As in the Java client, where the tracker is not a loop that an exception could stop: only the request
            // that failed is lost, and the consumer that asked for a reply gets the failure.
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} failed to process a request", prefix)
                failRequest ex msg

        } :> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
                System.Threading.Volatile.Write(&stopCause, ex)
            else
                Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix)
            // nothing reads the mailbox any more, so what is queued is answered here, and what comes later by its caller
            mb.Writer.TryComplete() |> ignore
            let mutable msg = Unchecked.defaultof<GroupingTrackerMessage>
            while mb.Reader.TryRead(&msg) do
                replyAfterStop msg)
        |> ignore

    let timer = new Timer()
    let tryLaunchTimer() =
        if ackGroupTime <> TimeSpan.Zero then
            timer.Interval <- ackGroupTime.TotalMilliseconds
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> post mb Flush)
            timer.Start()
    do tryLaunchTimer()

    interface IAcknowledgmentsGroupingTracker with
        /// Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
        /// resent after a disconnection and for which the user has already sent an acknowledgement.
        member this.IsDuplicate(msgId) =
            let channel = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
            postAndReply (GroupingTrackerMessage.IsDuplicate (msgId, channel))
            channel.Task
        member this.AddAcknowledgment(msgId, ackType, properties) =
            post mb (GroupingTrackerMessage.AddAcknowledgment (msgId, ackType, properties))
        member this.AddBatchIndexAcknowledgment(msgId, ackType, properties) =
            post mb (GroupingTrackerMessage.AddBatchIndexAcknowledgment (msgId, ackType, properties))
        member this.Flush () =
            post mb GroupingTrackerMessage.Flush
        member this.FlushAndClean() =
            post mb GroupingTrackerMessage.FlushAndClean
        member this.FlushAsync connectionState =
            let channel = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            postAndReply (FlushAsync (connectionState, channel))
            channel.Task
        member this.Close() =
            timer.Stop()
            post mb GroupingTrackerMessage.Stop


    static member NonPersistentAcknowledgmentGroupingTracker =
        {
            new IAcknowledgmentsGroupingTracker with
                member this.IsDuplicate(msgId) = falseTask
                member this.AddAcknowledgment(msgId, ackType, properties) = ()
                member this.AddBatchIndexAcknowledgment(msgId, ackType, properties) = ()
                member this.Flush() = ()
                member this.FlushAndClean() = ()
                member this.FlushAsync _ = unitTask
                member this.Close() = ()
        }

