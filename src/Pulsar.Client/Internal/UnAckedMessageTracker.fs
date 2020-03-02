namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers

type internal TrackerMessage =
    | Add of (MessageId*AsyncReplyChannel<bool>)
    | Remove of (MessageId*AsyncReplyChannel<bool>)
    | RemoveMessagesTill of (MessageId*AsyncReplyChannel<int>)
    | TickTime
    | Clear
    | Stop

type internal IUnAckedMessageTracker =
    abstract member Clear: unit -> unit
    abstract member Add: MessageId -> bool
    abstract member Remove: MessageId -> bool
    abstract member RemoveMessagesTill: MessageId -> int
    abstract member Close: unit -> unit

type internal UnAckedMessageTracker(prefix: string,
                                    ackTimeout: TimeSpan,
                                    tickDuration: TimeSpan,
                                    redeliverUnacknowledgedMessages: RedeliverSet -> unit,
                                    ?getTickScheduler: (unit -> unit) -> IDisposable) =

    let messageIdPartitionMap = SortedDictionary<MessageId, RedeliverSet>()
    let timePartitions = Queue<RedeliverSet>()
    let prefix = prefix + " Tracker"

    let fillTimePartions() =
        let stepsCount = Math.Ceiling(ackTimeout.TotalMilliseconds / tickDuration.TotalMilliseconds) |> int
        for _ in [1..stepsCount] do
            timePartitions.Enqueue(RedeliverSet())

    let mb = MailboxProcessor<TrackerMessage>.Start(fun inbox ->
        let rec loop (state: RedeliverSet)  =
            async {
                let! message = inbox.Receive()
                match message with

                | TrackerMessage.Add (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                    if messageIdPartitionMap.ContainsKey msgId then
                        channel.Reply(false)
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                    else
                        messageIdPartitionMap.Add(msgId, state)
                        state.Add(msgId) |> ignore
                        channel.Reply(true)
                    return! loop state

                | TrackerMessage.Remove (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Removing message {1}", prefix, msgId)
                    let mutable targetState: RedeliverSet = null
                    if messageIdPartitionMap.TryGetValue(msgId, &targetState) then
                        targetState.Remove(msgId) |> ignore
                        messageIdPartitionMap.Remove(msgId) |> ignore
                        channel.Reply(true)
                    else
                        Log.Logger.LogWarning("{0} Unexisting message remove {1}", prefix, msgId)
                        channel.Reply(false)
                    return! loop state

                | TrackerMessage.RemoveMessagesTill (msgId, channel) ->

                    Log.Logger.LogDebug("{0} RemoveMessagesTill {1}", prefix, msgId)
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
                    return! loop (RedeliverSet())

                | Clear ->

                    Log.Logger.LogDebug("{0} Clear", prefix)
                    messageIdPartitionMap.Clear()
                    timePartitions |> Seq.iter (fun partition -> partition.Clear())
                    return! loop (RedeliverSet())

                | Stop ->

                    Log.Logger.LogDebug("{0} Stop", prefix)
                    messageIdPartitionMap.Clear()
                    timePartitions.Clear()
            }
        loop (RedeliverSet())
    )

    let timer =
        fillTimePartions()
        match getTickScheduler with
        | None ->
            let timer = new Timer(tickDuration.TotalMilliseconds)
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> mb.Post TickTime)
            timer.Start() |> ignore
            timer :> IDisposable
        | Some getScheduler ->
            getScheduler(fun _ -> mb.Post TickTime)
    
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

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
            timer.Dispose()
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

