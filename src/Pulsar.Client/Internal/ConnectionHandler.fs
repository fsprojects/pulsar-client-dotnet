namespace Pulsar.Client.Internal

open System.Threading
open Pulsar.Client.Api
open Pulsar.Client.Common
open System
open FSharp.UMX
open System.Threading.Channels
open System.Threading.Tasks
open FSharp.Logf


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
                        lookup: BinaryLookupService,
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
                    logfw Log.Logger "%s{prefix} Client cnx already set for topic %A{topic}, ignoring reconnection request" prefix topic
                | _ ->
                    if isValidStateForReconnection() then
                        try
                            logfd Log.Logger "%s{prefix} Starting reconnect to %A{topic}" prefix topic
                            let! broker = lookup.GetBroker(topic)
                            let! clientCnx = connectionPool.GetConnection(broker, maxMessageSize, false)
                            this.ConnectionState <- Ready clientCnx
                            logfd Log.Logger "%s{prefix} Successfuly reconnected to %A{topic}, %A{clientCnx}" prefix topic clientCnx
                            connectionOpened epoch
                        with Flatten ex ->
                            match ex with
                            | MaxMessageSizeChanged newSize ->
                                logfd Log.Logger "%s{prefix} MaxMessageSizeChanged to %i{newSize}" prefix newSize
                                maxMessageSize <- newSize
                                post this.Mb GrabCnx
                            | _ ->
                                elogfw Log.Logger ex "%s{prefix} Error reconnecting to %A{topic} Current state %A{connectionState}" prefix topic this.ConnectionState
                                connectionFailed ex
                                if isValidStateForReconnection() then
                                    post this.Mb (ReconnectLater ex)
                    else
                        logfi Log.Logger "%s{prefix} Ignoring GrabCnx to %A{topic} Current state %A{connectionState}" prefix topic this.ConnectionState

            | ReconnectLater ex ->

                if isValidStateForReconnection() then
                    let delay = backoff.Next()
                    elogfw Log.Logger ex "%s{prefix} Could not get connection to %A{topic} Current state %A{connectionState} -- Will try again in %i{delay}ms"
                        prefix topic this.ConnectionState delay
                    this.ConnectionState <- Connecting
                    epoch <- epoch + 1UL
                    asyncDelayMs delay (fun() -> post this.Mb GrabCnx)
                else
                    logfi Log.Logger "%s{prefix} Ignoring ReconnectLater to %A{topic} Current state %A{connectionState}" prefix topic this.ConnectionState

            | ConnectionClosed clientCnx ->

                this.LastDisconnectedTimestamp <- %DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                match this.ConnectionState with
                | Ready cnx when cnx <> clientCnx ->
                    logfi Log.Logger "Closing %i{clientCnxId1} but %i{clientCnxId2} is already active" clientCnx.ClientCnxId cnx.ClientCnxId
                | _ ->
                    if isValidStateForReconnection() then
                        let delay = backoff.Next()
                        logfi Log.Logger "%s{prefix} Closed connection to %A{topic} Current state %A{connectionState} -- Will try again in %i{delay}ms"
                            prefix topic this.ConnectionState delay
                        this.ConnectionState <- Connecting
                        epoch <- epoch + 1UL
                        asyncDelayMs delay (fun() -> post this.Mb GrabCnx)
                    else
                        logfi Log.Logger "%s{prefix} Ignoring ConnectionClosed to %A{topic} Current state %A{connectionState}" prefix topic this.ConnectionState

            | Close ->
                continueLoop <- false
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                elogfc Log.Logger ex "%s{prefix} ConnectionHandler mailbox failure" prefix
            else
                logfi Log.Logger "%s{prefix} ConnectionHandler mailbox has stopped normally" prefix)
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