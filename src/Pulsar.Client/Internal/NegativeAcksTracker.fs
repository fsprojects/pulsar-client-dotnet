namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Collections.Generic
open System.Timers
open Microsoft.Extensions.Logging

type NegativeAcksTrackerMessage =
    | Add of (MessageId*AsyncReplyChannel<bool>)
    | TickTime
    | Stop

type NegativeAcksTracker (prefix: string, negativeAckRedeliveryDelay: TimeSpan, redeliverUnacknowledgedMessages: RedeliverSet -> unit) =

    let MIN_NACK_DELAY = TimeSpan.FromMilliseconds(100.0)
    let nackDelay = if negativeAckRedeliveryDelay > MIN_NACK_DELAY then negativeAckRedeliveryDelay else MIN_NACK_DELAY
    let timerIntervalms = nackDelay.TotalMilliseconds / 3.0
    let prefix = prefix + " NegativeTracker"

    let mb = MailboxProcessor<NegativeAcksTrackerMessage>.Start(fun inbox ->
        let rec loop (state: Map<MessageId, DateTime>)  =
            async {
                let! message = inbox.Receive()
                match message with

                | Add (msgId, channel) ->

                    Log.Logger.LogDebug("{0} Adding message {1}", prefix, msgId)
                    if state.ContainsKey(msgId) |> not then
                        let newState = state.Add(msgId, DateTime.Now.Add(nackDelay))
                        channel.Reply(true)
                        return! loop newState
                    else
                        Log.Logger.LogWarning("{0} Duplicate message add {1}", prefix, msgId)
                        channel.Reply(false)
                        return! loop state

                | TickTime ->
                    let parts = state |> Map.partition (fun _ value -> value < DateTime.Now)
                    let redelivery = parts |> fst |> Map.toSeq |> Seq.map fst |> HashSet<MessageId>
                    let remainder = parts |> snd

                    if redelivery.Count > 0 then
                        Log.Logger.LogDebug("{0} Redelivering {1} messages", prefix, redelivery.Count)
                        redeliverUnacknowledgedMessages redelivery

                    return! loop remainder
                | Stop ->
                    Log.Logger.LogDebug("{0} Stop", prefix)
            }
        loop (Map.empty<MessageId, DateTime>)
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    let timer = new Timer(timerIntervalms)
    do timer.AutoReset <- true
    do timer.Elapsed.Add(fun _ -> mb.Post TickTime)
    do timer.Start()

    member this.Add(msgId) =
        mb.PostAndReply (fun channel -> Add (msgId, channel))

    member this.Close() =
        timer.Stop()
        mb.Post Stop

