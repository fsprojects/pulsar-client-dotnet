module Pulsar.Client.Internal.ConnectionManager

open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive

let getConnection topic (lookup: ILookupService) =
    let topicName = TopicName(topic);
    task {
        let! broker = lookup.GetBroker(topicName)
        return! ConnectionPool.getConnectionAsync broker
    }

