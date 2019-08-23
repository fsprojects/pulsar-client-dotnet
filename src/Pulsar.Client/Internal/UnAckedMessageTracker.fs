namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers

type TrackerMessage =
    | Add of (MessageId*AsyncReplyChannel<bool>)
    | Remove of (MessageId*AsyncReplyChannel<bool>)
    | RemoveMessagesTill of (MessageId*AsyncReplyChannel<int>)
    | TickTime
    | Clear
    | Stop

type IUnAckedMessageTracker =
    abstract member Clear: unit -> unit
    abstract member Add: MessageId -> bool
    abstract member Remove: MessageId -> bool
    abstract member RemoveMessagesTill: MessageId -> int
    abstract member Close: unit -> unit

type UnAckedMessageTracker(prefix: string, ackTimeout: TimeSpan, tickDuration: TimeSpan, redeliverUnacknowledgedMessages: TrackerState -> unit) =

    let messageIdPartitionMap = SortedDictionary<MessageId, TrackerState>()
    let timePartitions = Queue<TrackerState>()
    let prefix = prefix + " Tracker"

    let fillTimePartions() =
        let stepsCount = Math.Ceiling(ackTimeout.TotalMilliseconds / tickDuration.TotalMilliseconds) |> int
        for _ in [1..stepsCount] do
            timePartitions.Enqueue(TrackerState())

    let mb = MailboxProcessor<TrackerMessage>.Start(fun inbox ->
        let rec loop (state: TrackerState)  =
            async {
                let! message = inbox.Receive()
                match message with
                | TrackerMessage.Add (msgId, channel) ->
                    if messageIdPartitionMap.ContainsKey msgId then
                        channel.Reply(false)
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                    else
                        messageIdPartitionMap.Add(msgId, state)
                        state.Add(msgId) |> ignore
                        channel.Reply(true)
                    return! loop state
                | TrackerMessage.Remove (msgId, channel) ->
                    let mutable targetState: TrackerState = null
                    if messageIdPartitionMap.TryGetValue(msgId, &targetState) then
                        targetState.Remove(msgId) |> ignore
                        messageIdPartitionMap.Remove(msgId) |> ignore
                        channel.Reply(true)
                    else
                        Log.Logger.LogWarning("{0} Unexisting message remove {1}", prefix, msgId)
                        channel.Reply(false)
                    return! loop state
                | TrackerMessage.RemoveMessagesTill (msgId, channel) ->
                    messageIdPartitionMap.Keys
                        |> Seq.takeWhile (fun key -> key <= msgId)
                        |> Seq.toArray
                        |> Seq.map (fun key ->
                              let targetState = messageIdPartitionMap.[key]
                              targetState.Remove(key) |> ignore
                              messageIdPartitionMap.Remove(key) |> ignore)
                        |> Seq.length
                        |> channel.Reply
                    return! loop state
                | TickTime ->
                    timePartitions.Enqueue(state)
                    let timedOutMessages = timePartitions.Dequeue()
                    if timedOutMessages.Count > 0 then
                        Log.Logger.LogWarning("{0} {1} messages have timed-out", prefix, timedOutMessages.Count)
                        for msgId in timedOutMessages do
                            messageIdPartitionMap.Remove(msgId) |> ignore
                        redeliverUnacknowledgedMessages timedOutMessages
                    return! loop (TrackerState())
                | Clear ->
                    messageIdPartitionMap.Clear()
                    timePartitions.Clear()
                    fillTimePartions()
                    return! loop (TrackerState())
                | Stop ->
                    messageIdPartitionMap.Clear()
                    timePartitions.Clear()
            }
        loop (TrackerState())
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    let timer = new Timer(tickDuration.TotalMilliseconds)
    do fillTimePartions()
    do timer.AutoReset <- true
    do timer.Elapsed.Add(fun _ -> mb.Post TickTime)
    do timer.Start()

    interface IUnAckedMessageTracker with
        member this.Clear() =
            mb.Post TrackerMessage.Clear
        member this.Add(msgId) =
            mb.PostAndReply (fun channel -> TrackerMessage.Add (msgId, channel))
        member this.Remove(msgId) =
            mb.PostAndReply (fun channel -> TrackerMessage.Remove (msgId, channel))
        member this.RemoveMessagesTill(msgId) =
            mb.PostAndReply (fun channel -> TrackerMessage.RemoveMessagesTill (msgId, channel))
        member this.Close() =
            timer.Stop()
            mb.Post Stop


    static member UNACKED_MESSAGE_TRACKER_DISABLED =
        {
            new IUnAckedMessageTracker with
                member this.Clear() = ()
                member this.Add(msgId) = true
                member this.Remove(msgId) = true
                member this.RemoveMessagesTill(msgId) = 0
                member this.Close() = ()
        }

