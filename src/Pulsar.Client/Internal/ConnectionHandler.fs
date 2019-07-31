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

type ConnectionHandler( parentPrefix: string,
                        lookup: BinaryLookupService,
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
                        Log.Logger.LogWarning("{0} Client cnx already set for topic {1}, ignoring reconnection request", parentPrefix, topic);
                    | _ ->
                        if isValidStateForReconnection() then
                            try
                                Log.Logger.LogDebug("{0} Starting reconnect to {1}", parentPrefix, topic);
                                let! broker = lookup.GetBroker(topic) |> Async.AwaitTask
                                let! clientCnx = ConnectionPool.getConnection broker |> Async.AwaitTask
                                this.ConnectionState <- Ready clientCnx
                                connectionOpened()
                            with
                            | ex ->
                                Log.Logger.LogWarning(ex, "{0} Error reconnecting to {1} Current state {2}", parentPrefix, topic, this.ConnectionState)
                                connectionFailed ex
                                if isValidStateForReconnection() then
                                    this.Mb.Post(ReconnectLater ex)
                        else
                            Log.Logger.LogInformation("{0} Ignoring GrabCnx to {1} Current state {2}", parentPrefix, topic, this.ConnectionState)
                | ReconnectLater ex ->
                    if isValidStateForReconnection() then
                        // TODO backoff
                        let delay = 1000
                        Log.Logger.LogWarning(ex, "{0} Could not get connection to {1} Current state {2} -- Will try again in {3}s ",
                            parentPrefix, topic, this.ConnectionState, delay / 1000)
                        this.ConnectionState <- Connecting
                        asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                    else
                        Log.Logger.LogInformation("{0} Ignoring ReconnectLater to {1} Current state {2}", parentPrefix, topic, this.ConnectionState)
                | ConnectionClosed ->
                    if isValidStateForReconnection() then
                        // TODO backoff
                        let delay = 1000
                        Log.Logger.LogInformation("{0} Closed connection to {1} Current state {2} -- Will try again in {2}s ",
                            parentPrefix, topic, this.ConnectionState, delay / 1000)
                        this.ConnectionState <- Connecting
                        asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                    else
                        Log.Logger.LogInformation("{0} Ignoring ConnectionClosed to {1} Current state {2}", parentPrefix, topic, this.ConnectionState)
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
