namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading
open Pulsar.Client.Common
open System

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
    let mutable maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE
    let mutable epoch = 0UL
    let prefix = parentPrefix + " ConnectionHandler"

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
                        Log.Logger.LogWarning("{0} Client cnx already set for topic {1}, ignoring reconnection request", prefix, topic);
                    | _ ->
                        if isValidStateForReconnection() then
                            try
                                Log.Logger.LogDebug("{0} Starting reconnect to {1}", prefix, topic)
                                let! broker = lookup.GetBroker(topic) |> Async.AwaitTask
                                let! clientCnx = connectionPool.GetConnection(broker, maxMessageSize, false) |> Async.AwaitTask
                                this.ConnectionState <- Ready clientCnx
                                connectionOpened epoch
                            with
                            | :? AggregateException as ex when (ex.InnerException :? MaxMessageSizeChanged) ->
                                let newSize = (ex.InnerException :?> MaxMessageSizeChanged).Data0
                                Log.Logger.LogInformation("{0} MaxMessageSizeChanged to {1}", prefix, newSize)
                                maxMessageSize <- newSize
                                this.Mb.Post(GrabCnx)                                
                            | ex ->
                                Log.Logger.LogWarning(ex, "{0} Error reconnecting to {1} Current state {2}", prefix, topic, this.ConnectionState)
                                connectionFailed ex
                                if isValidStateForReconnection() then
                                    this.Mb.Post(ReconnectLater ex)
                        else
                            Log.Logger.LogInformation("{0} Ignoring GrabCnx to {1} Current state {2}", prefix, topic, this.ConnectionState)
                    return! loop ()
                            
                | ReconnectLater ex ->
                    
                    if isValidStateForReconnection() then
                        let delay = backoff.Next()
                        Log.Logger.LogWarning(ex, "{0} Could not get connection to {1} Current state {2} -- Will try again in {3}ms ",
                            prefix, topic, this.ConnectionState, delay)
                        this.ConnectionState <- Connecting
                        epoch <- epoch + 1UL
                        asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                    else
                        Log.Logger.LogInformation("{0} Ignoring ReconnectLater to {1} Current state {2}", prefix, topic, this.ConnectionState)
                    return! loop ()
                        
                | ConnectionClosed clientCnx ->
                    
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
                            asyncDelay delay (fun() -> this.Mb.Post(GrabCnx))
                        else
                            Log.Logger.LogInformation("{0} Ignoring ConnectionClosed to {1} Current state {2}", prefix, topic, this.ConnectionState)
                    return! loop ()
                    
                | Close ->
                    ()
            }
        loop ()
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} ConnectionHandler mailbox failure", prefix))

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member this.GrabCnx() =
        mb.Post(GrabCnx)

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
        mb.Post(ConnectionClosed clientCnx)

    member this.ReconnectLater ex =
        mb.Post(ReconnectLater ex)

    member this.ResetBackoff() =
        backoff.Reset()

    member this.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and private set(value) = Volatile.Write(&connectionState, value)

    member this.IsRetriableError ex =
        match ex with
        | LookupException _ -> true
        | _ -> false

    member this.CheckIfActive() =
        match this.ConnectionState with
        | Ready _ | Connecting -> null
        | Closing | Closed -> AlreadyClosedException(prefix + "already closed")
        | Terminated -> AlreadyClosedException(prefix + " topic was terminated")
        | Failed | Uninitialized -> NotConnectedException(prefix + " not connected")
        
    member this.Close() =
        mb.Post(Close)