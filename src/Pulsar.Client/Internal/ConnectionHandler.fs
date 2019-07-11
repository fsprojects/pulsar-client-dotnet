namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading

type ConnectionHandlerMessage =
    | Connect of AsyncReplyChannel<unit>
    | Reconnect
    | Disconnected of AsyncReplyChannel<unit>

type ConnectionState =
    | Ready of ClientCnx
    | Connecting
    | Closing
    | Closed
    | Terminated
    | Failed
    | Uninitialized

type ConnectionHandler(registerFunction) as this =

    let mutable connectionState = Uninitialized
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
                    let! clientCnx = registerFunction()
                    connectionState <- Ready clientCnx
                    channel.Reply()
                | Reconnect ->
                    // TODO backoff
                    if isValidStateForReconnection connectionState
                    then
                        try
                            let! clientCnx = registerFunction()
                            connectionState <- Ready clientCnx
                            reconnectCount <- 0
                        with
                        | ex ->
                            this.Mb.Post(ConnectionHandlerMessage.Reconnect)
                            Log.Logger.LogError(ex, "Error reconnecting")
                            if reconnectCount > 3
                            then
                                raise ex
                | Disconnected channel ->
                    connectionState <- Connecting
                    this.Mb.Post(Reconnect)
                    channel.Reply()
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.Connect() =
        mb.PostAndAsyncReply(Connect)

    member __.Disconnected() =
        mb.PostAndAsyncReply(Disconnected)

    member __.ConnectionState with get() = connectionState
