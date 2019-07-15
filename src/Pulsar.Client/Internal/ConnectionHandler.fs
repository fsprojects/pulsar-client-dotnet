namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading

type ConnectionHandlerMessage =
    | Reconnect
    | Terminate
    | StartClose
    | EndClose
    | ConnectionClosed of AsyncReplyChannel<unit>

type ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type ConnectionHandler(lookup: BinaryLookupService, topic: string, connectionOpened: unit -> unit) as this =

    let mutable connectionState = Uninitialized
    let mutable reconnectCount = 0

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
                            reconnectCount <- 0
                            connectionOpened()
                        with
                        | ex ->
                            // TODO backoff
                            this.Mb.Post(ConnectionHandlerMessage.Reconnect)
                            Log.Logger.LogWarning(ex, "Error reconnecting on try {0}", reconnectCount)
                            if Interlocked.Increment(&reconnectCount) > 3
                            then
                                raise ex
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
                | Terminate ->
                    this.ConnectionState <- Terminated
                | StartClose ->
                    this.ConnectionState <- Closing
                | EndClose ->
                    this.ConnectionState <- Closed
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.Connect() =
        mb.Post(Reconnect)

    member __.Terminate() =
        mb.Post(Terminate)

    member __.Closed() =
        mb.Post(EndClose)

    member __.Closing() =
        mb.Post(StartClose)

    member __.ConnectionClosed() =
        mb.PostAndAsyncReply(ConnectionClosed)

    member __.ConnectionState
        with get() = Volatile.Read(&connectionState)
        and private set(value) = Volatile.Write(&connectionState, value)
