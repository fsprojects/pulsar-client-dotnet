namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers

type internal UnackedTrackerMessage =
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
    let mutable currentPartition = RedeliverSet()
    let prefix = prefix + " UnackedTracker"

    let fillTimePartions() =
        let stepsCount = Math.Ceiling(ackTimeout.TotalMilliseconds / tickDuration.TotalMilliseconds) |> int
        for _ in [1..stepsCount] do
            timePartitions.Enqueue(RedeliverSet())

    let mb = MailboxProcessor<UnackedTrackerMessage>.Start(fun inbox ->
        let rec loop ()  =
            async {
                let! message = inbox.Receive()
                match message with

                | UnackedTrackerMessage.Add (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                    if messageIdPartitionMap.ContainsKey msgId then
                        channel.Reply(false)
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                    else
                        messageIdPartitionMap.Add(msgId, currentPartition)
                        currentPartition.Add(msgId) |> ignore
                        channel.Reply(true)
                    return! loop ()

                | UnackedTrackerMessage.Remove (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Removing message {1}", prefix, msgId)
                    let mutable targetState: RedeliverSet = null
                    if messageIdPartitionMap.TryGetValue(msgId, &targetState) then
                        targetState.Remove(msgId) |> ignore
                        messageIdPartitionMap.Remove(msgId) |> ignore
                        channel.Reply(true)
                    else
                        Log.Logger.LogWarning("{0} Unexisting message remove {1}", prefix, msgId)
                        channel.Reply(false)
                    return! loop ()

                | UnackedTrackerMessage.RemoveMessagesTill (msgId, channel) ->

                    Log.Logger.LogDebug("{0} RemoveMessagesTill {1}", prefix, msgId)
                    let keysToRemove = 
                        messageIdPartitionMap.Keys
                        |> Seq.takeWhile (fun key -> key <= msgId)
                        |> Seq.toArray // materialize before removal operations
                    for key in keysToRemove do
                        let targetState = messageIdPartitionMap.[key]
                        targetState.Remove(key) |> ignore
                        messageIdPartitionMap.Remove(key) |> ignore
                    channel.Reply keysToRemove.Length
                    return! loop ()

                | TickTime ->

                    timePartitions.Enqueue(currentPartition)
                    let timedOutMessages = timePartitions.Dequeue()
                    let messagesToRedeliver = HashSet<MessageId>()
                    if timedOutMessages.Count > 0 then
                        Log.Logger.LogWarning("{0} {1} messages have timed-out", prefix, timedOutMessages.Count)
                        for msgId in timedOutMessages do
                            messageIdPartitionMap.Remove(msgId) |> ignore
                            match msgId.ChunkMessageIds with
                            | Some msgIds ->
                                msgIds |> Array.iter (messagesToRedeliver.Add >> ignore)
                            | None ->
                                messagesToRedeliver.Add(msgId) |> ignore
                        redeliverUnacknowledgedMessages messagesToRedeliver
                    currentPartition <- RedeliverSet()
                    return! loop ()

                | Clear ->

                    Log.Logger.LogDebug("{0} Clear", prefix)
                    messageIdPartitionMap.Clear()
                    for partition in timePartitions do
                        partition.Clear()
                    currentPartition.Clear()
                    return! loop ()

                | Stop ->

                    Log.Logger.LogDebug("{0} Stop", prefix)
                    messageIdPartitionMap.Clear()
                    timePartitions.Clear()
                    currentPartition.Clear()
            }
        loop ()
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
    
    do mb.Error.Add(fun ex ->
        Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    interface IUnAckedMessageTracker with
        member this.Clear() =
            mb.Post UnackedTrackerMessage.Clear
        member this.Add(msgId) =
            mb.PostAndReply (fun channel -> UnackedTrackerMessage.Add (msgId, channel))
        member this.Remove(msgId) =
            mb.PostAndReply (fun channel -> UnackedTrackerMessage.Remove (msgId, channel))
        member this.RemoveMessagesTill(msgId) =
            mb.PostAndReply (fun channel -> UnackedTrackerMessage.RemoveMessagesTill (msgId, channel))
           
        
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

