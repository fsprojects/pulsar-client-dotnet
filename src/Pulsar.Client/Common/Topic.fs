namespace Pulsar.Client.Common

open FSharp.UMX

type TopicDomain =
    | Persistent
    | NonPersistent

type TopicName(topic, ?domain: TopicDomain, ?tenant: string, ?nameSpace: string ) = 
    let _domain = defaultArg domain Persistent
    let _tenant = defaultArg tenant "public"
    let _namespace = defaultArg nameSpace "default"
    let _topicDomain = 
        match _domain with
        | Persistent -> "persitent"
        | NonPersistent -> "non-persistent"
    let completeTopicName = sprintf "%s://%s/%s/%s" _topicDomain _tenant _namespace topic
    let partitionIndex = 1
    
    member __.PartitionInex 
        with get () = partitionIndex

    member __.CompleteTopicName
        with get() : CompleteTopicName = %completeTopicName