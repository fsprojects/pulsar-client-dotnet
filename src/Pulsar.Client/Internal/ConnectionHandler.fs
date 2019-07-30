namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading
open Pulsar.Client.Common

type ConnectionHandlerMessage =
    | Reconnect
    | ConnectionClosed of AsyncReplyChannel<unit>

type ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type ConnectionHandler(lookup: BinaryLookupService, topic: CompleteTopicName, connectionOpened: unit -> unit) as this =

    let mutable connectionState = Uninitialized

    let mb = MailboxProcessor<ConnectionHandlerMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Reconnect ->
                    match this.ConnectionState with
                    | Uninitialized | Connecting ->
                        try
                            let! broker = lookup.GetBroker(topic) |> Async.AwaitTask
                            let! clientCnx = ConnectionPool.getConnection broker |> Async.AwaitTask
                            this.ConnectionState <- Ready clientCnx
                            connectionOpened()
                        with
                        | ex ->
                            // TODO backoff
                            this.Mb.Post(ConnectionHandlerMessage.Reconnect)
                            Log.Logger.LogWarning(ex, "Error reconnecting")
                    | _ ->
                        Log.Logger.LogInformation("Skipped reconnecting for state {0} on Reconnect", this.ConnectionState)
                | ConnectionClosed channel ->
                    match this.ConnectionState with
                    | Uninitialized | Ready _ ->
                        this.ConnectionState <- Connecting
                        this.Mb.Post(Reconnect)
                    | _ ->
                        Log.Logger.LogInformation("Skipped reconnecting for state {0} on ConnectionClosed", this.ConnectionState)
                    channel.Reply()
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.Connect() =
        mb.Post(Reconnect)

    member __.Terminate() =
        __.ConnectionState <- Terminated

    member __.Closed() =
        __.ConnectionState <- Closed

    member __.Closing() =
        __.ConnectionState <- Closing

    member __.SetReady connection =
        __.ConnectionState <- Ready connection

    member __.ConnectionClosed() =
        mb.PostAndAsyncReply(ConnectionClosed)

    member __.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and private set(value) = Volatile.Write(&connectionState, value)
