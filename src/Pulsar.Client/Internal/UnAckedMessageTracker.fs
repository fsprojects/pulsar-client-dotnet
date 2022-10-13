namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open System.Timers
open System.Threading.Channels
open System.Threading.Tasks


type internal UnackedTrackerMessage =
    | Add of (MessageId*TaskCompletionSource<bool>)
    | Remove of (MessageId*TaskCompletionSource<bool>)
    | RemoveMessagesTill of (MessageId*TaskCompletionSource<int>)
    | TickTime
    | Clear
    | Stop

type internal IUnAckedMessageTracker =
    abstract member Clear: unit -> unit
    abstract member Add: MessageId -> Task<bool>
    abstract member Remove: MessageId -> Task<bool>
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
            match! mb.Reader.ReadAsync() with
            | UnackedTrackerMessage.Add (msgId, channel) ->

                Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                if messageIdPartitionMap.ContainsKey msgId then
                    channel.SetResult(false)
                    Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                else
                    messageIdPartitionMap.Add(msgId, currentPartition)
                    currentPartition.Add(msgId) |> ignore
                    channel.SetResult(true)

            | UnackedTrackerMessage.Remove (msgId, channel) ->

                Log.Logger.LogDebug("{0} Removing message {1}", prefix, msgId)
                let mutable targetState: RedeliverSet = null
                if messageIdPartitionMap.TryGetValue(msgId, &targetState) then
                    targetState.Remove(msgId) |> ignore
                    messageIdPartitionMap.Remove(msgId) |> ignore
                    channel.SetResult(true)
                else
                    Log.Logger.LogDebug("{0} Unexisting message remove {1}", prefix, msgId)
                    channel.SetResult(false)

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
                currentPartition <- RedeliverSet()

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

        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
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
            postAndAsyncReply mb (fun channel -> UnackedTrackerMessage.Add (msgId, channel))
        member this.Remove(msgId) =
            postAndAsyncReply mb (fun channel -> UnackedTrackerMessage.Remove (msgId, channel))
        member this.RemoveMessagesTill(msgId) =
            postAndAsyncReply mb (fun channel -> UnackedTrackerMessage.RemoveMessagesTill (msgId, channel))


        member this.Close() =
            timer.Dispose()
            post mb Stop


    static member UNACKED_MESSAGE_TRACKER_DISABLED =
        {
            new IUnAckedMessageTracker with
                member this.Clear() = ()
                member this.Add(msgId) = trueTask
                member this.Remove(msgId) = trueTask
                member this.RemoveMessagesTill(msgId) = zeroTask
                member this.Close() = ()
        }

