namespace Pulsar.Client.Internal

open Microsoft.Extensions.Logging
open System.Threading

type ConnectionHandlerMessage =
    | Connect of AsyncReplyChannel<unit>
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

type ConnectionHandler(registerFunction) as this =

    let mutable connectionState = Uninitialized
    let mutable reconnectCount = 0

    let isValidStateForReconnection() =
        match connectionState with
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
                    if isValidStateForReconnection()
                    then
                        try
                            let! clientCnx = registerFunction()
                            connectionState <- Ready clientCnx
                            reconnectCount <- 0
                        with
                        | ex ->
                            // TODO backoff
                            this.Mb.Post(ConnectionHandlerMessage.Reconnect)
                            Log.Logger.LogWarning(ex, "Error reconnecting")
                            if reconnectCount > 3
                            then
                                raise ex
                    else
                        // Ignore connection closed when we are shutting down
                        Log.Logger.LogInformation("Skipped reconnecting for state {0} on Reconnect", connectionState)
                | ConnectionClosed channel ->
                    if isValidStateForReconnection()
                    then
                        connectionState <- Connecting
                        this.Mb.Post(Reconnect)
                    else
                        Log.Logger.LogInformation("Skipped reconnecting for state {0} on ConnectionClosed", connectionState)
                    channel.Reply()
                return! loop ()
            }
        loop  ()
    )

    member private __.Mb with get() : MailboxProcessor<ConnectionHandlerMessage> = mb

    member __.Connect() =
        mb.PostAndAsyncReply(Connect)

    member __.ConnectionClosed() =
        mb.PostAndAsyncReply(ConnectionClosed)

    member __.ConnectionState with get() = connectionState
