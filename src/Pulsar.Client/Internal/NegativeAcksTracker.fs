namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Collections.Generic
open System.Timers
open Microsoft.Extensions.Logging

type internal NegativeAcksTrackerMessage =
    | Add of (MessageId*AsyncReplyChannel<bool>)
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

    let mb = MailboxProcessor<NegativeAcksTrackerMessage>.Start(fun inbox ->
        let rec loop (state: SortedDictionary<MessageId, DateTime>)  =
            async {
                let! message = inbox.Receive()
                match message with

                | Add (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                    if state.ContainsKey(msgId) |> not then
                        state.Add(msgId, DateTime.Now.Add(nackDelay))
                        channel.Reply(true)
                    else
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                        channel.Reply(false)
                    return! loop state

                | TickTime ->

                    if state.Count > 0 then
                        let result = HashSet<MessageId>()
                        for item in state do
                            if item.Value < DateTime.Now then
                                result.Add(item.Key) |> ignore
                        if result.Count > 0 then
                            for itemToRemove in result do
                                state.Remove(itemToRemove) |> ignore
                            Log.Logger.LogDebug("{0} Redelivering {1} messages", prefix, result.Count)
                            redeliverUnacknowledgedMessages result
                    else
                        ()
                    return! loop state

                | Stop ->

                    Log.Logger.LogDebug("{0} Stop", prefix)
                    state.Clear()
            }
        loop (SortedDictionary<MessageId, DateTime>())
    )

    let timer =
        match getTickScheduler with
        | None ->
            let timer = new Timer(timerIntervalms)
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> mb.Post TickTime)
            timer.Start() |> ignore
            timer :> IDisposable
        | Some getScheduler ->
            getScheduler(fun _ -> mb.Post TickTime)
    
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    member this.Add(msgId) =
        mb.PostAndReply (fun channel -> Add (msgId, channel))

    member this.Close() =
        timer.Dispose()
        mb.Post Stop

