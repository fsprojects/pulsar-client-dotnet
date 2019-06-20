namespace Pulsar.Client.Internal

open Pipelines.Sockets.Unofficial
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open pulsar.proto
open FSharp.UMX
open Pulsar.Client.Api
open System
open System.IO.Pipelines
open System.Threading.Tasks

type ConnectionState =
    | NotConnected
    | Connected of SocketConnection    

type ConnectionHandler private(topic, lookup: BinaryLookupService) =
    let messageDeliveredEvent = Event<SendAck>()
    let messageReceivedEvent = Event<Message>()
    let mutable state = NotConnected

    static member Init(topic, lookup: BinaryLookupService) =
        task {
            let ch = ConnectionHandler(topic, lookup)
            let! conn = ch.GrabCnx(topic, lookup)
            return ch
        }          

    member private __.GrabCnx (topic, lookup: BinaryLookupService) =
        task {
            let topicName = TopicName(topic)
            let! broker = lookup.GetBroker(topicName)
            let! conn = SocketManager.getConnection broker
            state <- Connected conn
            return conn
        }

    member __.MessageDelivered = messageDeliveredEvent.Publish

    member __.MessageReceived = messageReceivedEvent.Publish

    member __.Send (payload) =
        task {
            match state with
            | Connected socket -> 
                return! SocketManager.send (socket, payload)
            | NotConnected ->
                let! newSocket = __.GrabCnx(topic, lookup)
                return! SocketManager.send (newSocket, payload)
        }
    

        
        

