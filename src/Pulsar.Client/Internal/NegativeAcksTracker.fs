namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Collections.Generic
open System.Timers
open Microsoft.Extensions.Logging
open System.Threading.Channels
open System.Threading.Tasks


type internal NegativeAcksTrackerMessage =
    | Add of (MessageId*TaskCompletionSource<bool>)
    | TickTime
    | Stop

type internal NegativeAcksTracker(prefix: string,
                                  negativeAckRedeliveryDelay: TimeSpan,
                                  redeliverUnacknowledgedMessages: RedeliverSet -> unit,
                                  ?getTickScheduler: (unit -> unit) -> IDisposable) =

    let MIN_NACK_DELAY = TimeSpan.FromMilliseconds(100.0)
    let nackDelay = if negativeAckRedeliveryDelay > MIN_NACK_DELAY then negativeAckRedeliveryDelay else MIN_NACK_DELAY
    let timerIntervalms = nackDelay.TotalMilliseconds / 3.0
    let prefix = prefix + " NegativeTracker"
    let state = SortedDictionary<MessageId, DateTime>()


    let mb = Channel.CreateUnbounded<NegativeAcksTrackerMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! mb.Reader.ReadAsync() with
            | Add (msgId, channel) ->

                Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                if state.ContainsKey(msgId) |> not then
                    state.Add(msgId, DateTime.Now.Add(nackDelay))
                    channel.SetResult(true)
                else
                    Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                    channel.SetResult(false)

            | TickTime ->

                if state.Count > 0 then
                    let messagesToRedeliver = HashSet<MessageId>()
                    for KeyValue(messageId, expirationDate) in state do
                        if expirationDate < DateTime.Now then
                            match messageId.ChunkMessageIds with
                            | Some msgIds ->
                                msgIds |> Array.iter (messagesToRedeliver.Add >> ignore)
                            | None ->
                                messagesToRedeliver.Add(messageId) |> ignore
                    if messagesToRedeliver.Count > 0 then
                        for msgId in messagesToRedeliver do
                            state.Remove(msgId) |> ignore
                        Log.Logger.LogDebug("{0} Redelivering {1} messages", prefix, messagesToRedeliver.Count)
                        redeliverUnacknowledgedMessages messagesToRedeliver
                else
                    ()

            | Stop ->

                Log.Logger.LogDebug("{0} Stop", prefix)
                state.Clear()
                continueLoop <- false
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    let timer =
        match getTickScheduler with
        | None ->
            let timer = new Timer(timerIntervalms)
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> post mb TickTime)
            timer.Start() |> ignore
            timer :> IDisposable
        | Some getScheduler ->
            getScheduler(fun _ -> post mb TickTime)

    member this.Add(msgId) =
        postAndAsyncReply mb (fun channel -> Add (msgId, channel))

    member this.Close() =
        timer.Dispose()
        post mb Stop

