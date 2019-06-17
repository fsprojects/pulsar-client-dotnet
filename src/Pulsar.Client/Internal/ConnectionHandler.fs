namespace Pulsar.Client.Internal

open Pipelines.Sockets.Unofficial
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open pulsar.proto
open FSharp.UMX
open Pulsar.Client.Api

type ConnectionHandler() =
    let connectionOpenedEvent = Event<SocketConnection>()
    let connectionClosedEvent = Event<SocketConnection>()
    let messageDeliveredEvent = Event<SendAck>()
    let messageReceivedEvent = Event<Message>()

    member __.GrabCnx topic (lookup: BinaryLookupService)  =
        task {
            let topicName = TopicName(topic)
            let! broker = lookup.GetBroker(topicName)
            let! conn = SocketManager.getConnection broker
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

    member __.ConnectionOpened = connectionOpenedEvent.Publish

    member __.ConnectionClosed = connectionClosedEvent.Publish

    member __.MessageDelivered = messageDeliveredEvent.Publish

    member __.MessageReceived = messageReceivedEvent.Publish
        

