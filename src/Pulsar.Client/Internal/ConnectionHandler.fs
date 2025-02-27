namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading
open Pulsar.Client.Api
open Pulsar.Client.Common
open System
open FSharp.UMX
open System.Threading.Channels
open System.Threading.Tasks


type internal ConnectionHandlerMessage =
    | GrabCnx
    | ReconnectLater of exn
    | ConnectionClosed of ClientCnx
    | Close

type internal ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type internal ConnectionHandler( parentPrefix: string,
                        connectionPool: ConnectionPool,
                        lookup: ILookupService,
                        topic: CompleteTopicName,
                        connectionOpened: uint64 -> unit,
                        connectionFailed: exn -> unit,
                        backoff: Backoff) as this =

    let mutable connectionState = Uninitialized
    let mutable lastDisconnectedTimestamp = 0L
    let mutable maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE
    let mutable epoch = 0UL
    let prefix = parentPrefix + " ConnectionHandler"

    let isValidStateForReconnection() =
        match this.ConnectionState with
        | Uninitialized | Connecting | Ready _ -> true
        | _ -> false

    let mb = Channel.CreateUnbounded<ConnectionHandlerMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            match! mb.Reader.ReadAsync() with
            | GrabCnx ->

                match this.ConnectionState with
                | Ready _ ->
                    Log.Logger.LogWarning("{0} Client cnx already set for topic {1}, ignoring reconnection request", prefix, topic);
                | _ ->
                    if isValidStateForReconnection() then
                        try
                            Log.Logger.LogDebug("{0} Starting reconnect to {1}", prefix, topic)
                            let! broker = lookup.GetBroker(topic)
                            let! clientCnx = connectionPool.GetConnection(broker, maxMessageSize)
                            this.ConnectionState <- Ready clientCnx
                            Log.Logger.LogDebug("{0} Successfuly reconnected to {1}, {2}", prefix, topic, clientCnx)
                            connectionOpened epoch
                        with Flatten ex ->
                            Log.Logger.LogWarning(ex, "{0} Error reconnecting to {1} Current state {2}", prefix, topic, this.ConnectionState)
                            connectionFailed ex
                            if isValidStateForReconnection() then
                                post this.Mb (ReconnectLater ex)
                    else
                        Log.Logger.LogInformation("{0} Ignoring GrabCnx to {1} Current state {2}", prefix, topic, this.ConnectionState)

            | ReconnectLater ex ->

                if isValidStateForReconnection() then
                    let delay = backoff.Next()
                    Log.Logger.LogWarning(ex, "{0} Could not get connection to {1} Current state {2} -- Will try again in {3}ms ",
                        prefix, topic, this.ConnectionState, delay)
                    this.ConnectionState <- Connecting
                    epoch <- epoch + 1UL
                    asyncDelayMs delay (fun() -> post this.Mb GrabCnx)
                else
                    Log.Logger.LogInformation("{0} Ignoring ReconnectLater to {1} Current state {2}", prefix, topic, this.ConnectionState)

            | ConnectionClosed clientCnx ->

                this.LastDisconnectedTimestamp <- %DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                match this.ConnectionState with
                | Ready cnx when cnx <> clientCnx ->
                    Log.Logger.LogInformation("Closing {0} but {1} is already active", clientCnx.ClientCnxId, cnx.ClientCnxId)
                | _ ->
                    if isValidStateForReconnection() then
                        let delay = backoff.Next()
                        Log.Logger.LogInformation("{0} Closed connection to {1} Current state {2} -- Will try again in {3}ms ",
                            prefix, topic, this.ConnectionState, delay)
                        this.ConnectionState <- Connecting
                        epoch <- epoch + 1UL
                        asyncDelayMs delay (fun() -> post this.Mb GrabCnx)
                    else
                        Log.Logger.LogInformation("{0} Ignoring ConnectionClosed to {1} Current state {2}", prefix, topic, this.ConnectionState)

            | Close ->
                continueLoop <- false
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} ConnectionHandler mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} ConnectionHandler mailbox has stopped normally", prefix))
    |> ignore

    member private __.Mb with get() : Channel<ConnectionHandlerMessage> = mb

    member this.GrabCnx() =
        post mb GrabCnx

    member this.Terminate() =
        this.ConnectionState <- Terminated

    member this.Closed() =
        this.ConnectionState <- Closed

    member this.Failed() =
        this.ConnectionState <- Failed

    member this.Closing() =
        this.ConnectionState <- Closing

    member this.SetReady connection =
        this.ConnectionState <- Ready connection

    member this.ConnectionClosed (clientCnx: ClientCnx) =
        post mb (ConnectionClosed clientCnx)

    member this.ReconnectLater ex =
        post mb (ReconnectLater ex)

    member this.ResetBackoff() =
        backoff.Reset()

    member this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and private set(value) = Volatile.Write(&connectionState, value)

    member this.LastDisconnectedTimestamp
        with get() : TimeStamp = %(Volatile.Read(&lastDisconnectedTimestamp))
        and private set(value: TimeStamp) = Volatile.Write(&lastDisconnectedTimestamp, %value)

    member this.CheckIfActive() =
        match this.ConnectionState with
        | Ready _ | Connecting -> null
        | Closing | Closed -> AlreadyClosedException(prefix + "already closed") :> exn
        | Terminated -> AlreadyClosedException(prefix + " topic was terminated") :> exn
        | Failed | Uninitialized -> NotConnectedException(prefix + " not connected") :> exn

    member this.Close() =
        post mb Close