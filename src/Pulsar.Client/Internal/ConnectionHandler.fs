namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging

type ConnectionHandlerMessage =
    | Connect of AsyncReplyChannel<unit>
    | Reconnect
    | Disconnected

type ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type ConnectionHandler(registerFunction) as this =

    let mutable connectionState = ConnectionState.Uninitialized
    let mutable reconnectCount = 0

    let isValidStateForReconnection state =
        match state with
        | Uninitialized | Connecting | Ready _ ->
            // Ok
            true

        | Closing | Closed | Failed | Terminated ->
            false

    let mb = MailboxProcessor<ConnectionHandlerMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Connect channel ->
                    let! clientCnx = registerFunction() |> Async.AwaitTask
                    connectionState <- Ready clientCnx
                    channel.Reply()
                | Reconnect ->
                    // TODO backoff
                    if isValidStateForReconnection connectionState
                    then
                        try
                            let! clientCnx = registerFunction() |> Async.AwaitTask
                            connectionState <- Ready clientCnx
                            reconnectCount <- 0
                        with
                        | ex ->
                            this.Mb.Post(ConnectionHandlerMessage.Reconnect)
                            Log.Logger.LogError(ex, "Error reconnecting")
                            if reconnectCount > 3
                            then
                                raise ex
                | Disconnected ->
                    this.Mb.Post(Reconnect)
                    connectionState <- Connecting
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.Connect() =
        mb.PostAndAsyncReply(Connect)

    member __.Reconnect() =
        mb.Post(Reconnect)

    member __.Disconnected() =
        mb.Post(Disconnected)

    member __.ConnectionState with get() = connectionState
