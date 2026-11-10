namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers
open System.Threading.Channels
open System.Threading.Tasks


type internal UnackedTrackerMessage =
    | Add of MessageId
    | Remove of MessageId
    | RemoveMessagesTill of (MessageId*TaskCompletionSource<int>)
    | TickTime
    | Clear
    | Stop

type internal IUnAckedMessageTracker =
    abstract member Clear: unit -> unit
    abstract member Add: MessageId -> unit
    abstract member Remove: MessageId -> unit
    abstract member RemoveMessagesTill: MessageId -> Task<int>
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

    let mb = Channel.CreateUnbounded<UnackedTrackerMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            try
                match msg with
                | UnackedTrackerMessage.Add msgId ->

                    Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                    if messageIdPartitionMap.ContainsKey msgId then
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                    else
                        messageIdPartitionMap.Add(msgId, currentPartition)
                        currentPartition.Add(msgId) |> ignore

                | UnackedTrackerMessage.Remove msgId ->

                    Log.Logger.LogDebug("{0} Removing message {1}", prefix, msgId)
                    let mutable targetState: RedeliverSet = null
                    if messageIdPartitionMap.TryGetValue(msgId, &targetState) then
                        targetState.Remove(msgId) |> ignore
                        messageIdPartitionMap.Remove(msgId) |> ignore
                    else
                        Log.Logger.LogDebug("{0} Unexisting message remove {1}", prefix, msgId)

                | UnackedTrackerMessage.RemoveMessagesTill (msgId, channel) ->

                    Log.Logger.LogDebug("{0} RemoveMessagesTill {1}", prefix, msgId)
                    let keysToRemove =
                        messageIdPartitionMap.Keys
                        |> Seq.takeWhile (fun key -> key <= msgId)
                        |> Seq.toArray // materialize before removal operations
                    for key in keysToRemove do
                        let targetState = messageIdPartitionMap[key]
                        targetState.Remove(key) |> ignore
                        messageIdPartitionMap.Remove(key) |> ignore
                    channel.SetResult keysToRemove.Length

                | TickTime ->

                    timePartitions.Enqueue(currentPartition)
                    // before the redelivery, which is the part that can fail, as in the Java client
                    currentPartition <- RedeliverSet()
                    let timedOutMessages = timePartitions.Dequeue()
                    if timedOutMessages.Count > 0 then
                        let messagesToRedeliver = HashSet<MessageId>()
                        Log.Logger.LogWarning("{0} {1} messages have timed-out", prefix, timedOutMessages.Count)
                        for msgId in timedOutMessages do
                            messageIdPartitionMap.Remove(msgId) |> ignore
                            match msgId.ChunkMessageIds with
                            | Some msgIds ->
                                msgIds |> Array.iter (messagesToRedeliver.Add >> ignore)
                            | None ->
                                messagesToRedeliver.Add(msgId) |> ignore
                        redeliverUnacknowledgedMessages messagesToRedeliver

                | Clear ->

                    Log.Logger.LogDebug("{0} Clear", prefix)
                    messageIdPartitionMap.Clear()
                    for partition in timePartitions do
                        partition.Clear()
                    currentPartition.Clear()

                | Stop ->

                    Log.Logger.LogDebug("{0} Stop", prefix)
                    messageIdPartitionMap.Clear()
                    timePartitions.Clear()
                    currentPartition.Clear()
                    continueLoop <- false

            // As in the Java client, where a failed tick is followed by the next one: the tracker goes on, or
            // messages would no longer be redelivered after their ack timeout without anybody noticing.
            with ex ->
                Log.Logger.LogError(ex, "{0} failed to process a request", prefix)
                match msg with
                | UnackedTrackerMessage.RemoveMessagesTill (_, channel) -> channel.TrySetException ex |> ignore
                | _ -> ()

        }:> Task).ContinueWith(fun t ->
            let ex = loopFailure t
            if not (isNull ex) then
                Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    do fillTimePartions()
    let timer =
        match getTickScheduler with
        | None ->
            let timer = new Timer(tickDuration.TotalMilliseconds)
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> post mb TickTime)
            timer.Start()
            timer :> IDisposable
        | Some getScheduler ->
            getScheduler(fun _ -> post mb TickTime)

    interface IUnAckedMessageTracker with
        member this.Clear() =
            post mb UnackedTrackerMessage.Clear
        member this.Add(msgId) =
            post mb (UnackedTrackerMessage.Add msgId)
        member this.Remove(msgId) =
            post mb (UnackedTrackerMessage.Remove msgId)
        member this.RemoveMessagesTill(msgId) =
            postAndAsyncReply mb (fun channel -> UnackedTrackerMessage.RemoveMessagesTill (msgId, channel))


        member this.Close() =
            timer.Dispose()
            post mb Stop


    static member UNACKED_MESSAGE_TRACKER_DISABLED =
        {
            new IUnAckedMessageTracker with
                member this.Clear() = ()
                member this.Add(msgId) = ()
                member this.Remove(msgId) = ()
                member this.RemoveMessagesTill(msgId) = zeroTask
                member this.Close() = ()
        }

