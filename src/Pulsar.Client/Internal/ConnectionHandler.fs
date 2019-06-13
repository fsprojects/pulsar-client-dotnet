namespace Pulsar.Client.Internal

open Pipelines.Sockets.Unofficial
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open pulsar.proto
open FSharp.UMX
open Pulsar.Client.Api

type ConnectionHandler() =
    let connectionOpenedEvent = Event<SocketConnection>()
    let messageDeliveredEvent = Event<SendAck>()
    let messageReceivedEvent = Event<Message>()

    member __.GrabCnx topic lookup  =
        task {
            let! conn = ConnectionManager.getConnection topic lookup
            connectionOpenedEvent.Trigger(conn)
            __.StartListening conn
        }

    member __.StartListening conn =
        let receipt = new CommandSendReceipt()
        messageDeliveredEvent.Trigger(
            {
                SequenceId = % receipt.SequenceId
                LedgerId = % receipt.MessageId.ledgerId
                EntryId= % receipt.MessageId.entryId 
            }
        )

    [<CLIEvent>]
    member __.ConnectionOpened = connectionOpenedEvent.Publish

    [<CLIEvent>]
    member __.MessageDelivered = messageDeliveredEvent.Publish

    [<CLIEvent>]
    member __.MessageReceived = messageReceivedEvent.Publish
        

