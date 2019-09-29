namespace Pulsar.Client.Common

open FSharp.UMX
open System.Text.RegularExpressions
open System

module internal TopicNameHelpers =
    let FullTopicRegex = Regex(@"^(persistent|non-persistent):\/\/([^\/]+)\/([^\/]+)\/([^\/]+)$")
    let DefaultDomain = "persistent"
    let DefaultTenant = "public"
    let DefaultNamespace = "default"
    let PartitionTopicSuffix = "-partition-"

// Full name example - persistent://tenant/namespace/topic
// The short topic name can be:
// - <topic>
// - <property>/<namespace>/<topic>

open TopicNameHelpers

type TopicName private (completeTopicName: string, partition: int) =

    let isPersistent = completeTopicName.StartsWith("persistent")
    let isPartitioned = partition > -1
    let completeTopicName =
        if isPartitioned then
            completeTopicName + PartitionTopicSuffix + partition.ToString()
        else
            completeTopicName

    new(topic: string) =
        let completeTopicName =
            if String.IsNullOrWhiteSpace(topic)
            then failwith "Topic name should not be empty"
            else
                if FullTopicRegex.IsMatch(topic)
                then topic
                else
                    let parts = topic.Split('/')
                    if (parts.Length = 3)
                    then
                        sprintf "%s://%s" DefaultDomain topic
                    else
                        if (parts.Length = 1)
                        then
                            sprintf "%s://%s/%s/%s" DefaultDomain DefaultTenant DefaultNamespace topic
                        else
                            failwith "Invalid short topic name '" + topic + "', it should be in the format of <tenant>/<namespace>/<topic> or <topic>"
        TopicName(%completeTopicName, -1)

    member this.CompleteTopicName: CompleteTopicName = %completeTopicName

    member this.IsPersistent = isPersistent

    member this.IsPartitioned = isPartitioned

    member this.GetPartition(index: int) =
        if (index = -1 || isPartitioned) then
            this
        else
            TopicName(completeTopicName, index)

    override this.ToString() =
        %completeTopicName