namespace Pulsar.Client.Common

open FSharp.UMX
open System.Text.RegularExpressions
open System

module internal TopicNameHelpers =
    let FullTopicRegex = Regex(@"^(persistent|non-persistent):\/\/([^\/]+)\/([^\/]+)\/([^\/]+)$")
    let DefaultDomain = "persistent"
    let DefaultTenant = "public"
    let DefaultNamespace = "default"

// Full name example - persistent://tenant/namespace/topic
// The short topic name can be:
// - <topic>
// - <property>/<namespace>/<topic>

open TopicNameHelpers

type TopicName(topic: string) =

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

    member __.CompleteTopicName
        with get() : CompleteTopicName = %completeTopicName

    override this.ToString() = %this.CompleteTopicName