namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading
open Pulsar.Client.Common

type ConnectionHandlerMessage =
    | GrabCnx
    | ReconnectLater of exn
    | ConnectionClosed

type ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type ConnectionHandler( lookup: BinaryLookupService,
                        topic: CompleteTopicName,
                        connectionOpened: unit -> unit,
                        connectionFailed: exn -> unit) as this =

    let mutable connectionState = Uninitialized

    let isValidStateForReconnection() =
        match this.ConnectionState with
        | Uninitialized | Connecting | Ready _ -> true
        | _ -> false

    let mb = MailboxProcessor<ConnectionHandlerMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | GrabCnx ->
                    match this.ConnectionState with
                    | Ready _ ->
                        Log.Logger.LogWarning("Client cnx already set for topic {0}, ignoring reconnection request", topic);
                    | _ ->
                        if isValidStateForReconnection() then
                            try
                                let! broker = lookup.GetBroker(topic) |> Async.AwaitTask
                                let! clientCnx = ConnectionPool.getConnection broker |> Async.AwaitTask
                                this.ConnectionState <- Ready clientCnx
                                connectionOpened()
                            with
                            | ex ->
                                Log.Logger.LogWarning(ex, "Error reconnecting to {0} Current state {1}", topic, this.ConnectionState)
                                connectionFailed ex
                                if isValidStateForReconnection() then
                                    this.Mb.Post(ReconnectLater ex)
                        else
                            Log.Logger.LogInformation("Ignoring GrabCnx to {0} Current state {1}", topic, this.ConnectionState)
                | ReconnectLater ex ->
                    if isValidStateForReconnection() then
                        // TODO backoff
                        let delay = 1000
                        Log.Logger.LogWarning(ex, "Could not get connection to {0} Current state {1} -- Will try again in {2}s ",
                            topic, this.ConnectionState, delay / 1000)
                        this.ConnectionState <- Connecting
                        asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                    else
                        Log.Logger.LogInformation("Ignoring ReconnectLater to {0} Current state {1}", topic, this.ConnectionState)
                | ConnectionClosed ->
                    if isValidStateForReconnection() then
                        // TODO backoff
                        let delay = 1000
                        Log.Logger.LogInformation("Closed connection to {0} Current state {1} -- Will try again in {2}s ",
                            topic, this.ConnectionState, delay / 1000)
                        this.ConnectionState <- Connecting
                        asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                    else
                        Log.Logger.LogInformation("Ignoring ConnectionClosed to {0} Current state {1}", topic, this.ConnectionState)
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.GrabCnx() =
        mb.Post(GrabCnx)

    member __.Terminate() =
        __.ConnectionState <- Terminated

    member __.Closed() =
        __.ConnectionState <- Closed

    member __.Failed() =
        __.ConnectionState <- Failed

    member __.Closing() =
        __.ConnectionState <- Closing

    member __.SetReady connection =
        __.ConnectionState <- Ready connection

    member __.ConnectionClosed() =
        mb.Post(ConnectionClosed)

    member __.ReconnectLater ex =
        mb.Post(ReconnectLater ex)

    member __.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and private set(value) = Volatile.Write(&connectionState, value)

    member __.IsRetriableError ex =
        match ex with
        | LookupException _ -> true
        | _ -> false
