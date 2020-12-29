namespace Pulsar.Client.Common

open FSharp.UMX
open System.Text.RegularExpressions
open System

module internal TopicNameHelpers =
    let FullTopicRegex = Regex(@"^(persistent|non-persistent):\/\/([^\/]+)\/([^\/]+)\/([^\/]+)$")
    [<Literal>]
    let DefaultDomain = "persistent"
    [<Literal>]
    let DefaultTenant = "public"
    [<Literal>]
    let DefaultNamespace = "default"
    [<Literal>]
    let PartitionTopicSuffix = "-partition-"
    let GetPartitionIndex (completeTopicName: string) =
        if completeTopicName.Contains(PartitionTopicSuffix) then
            completeTopicName.Substring(completeTopicName.LastIndexOf('-') + 1) |> int
        else
            -1

// Full name example - persistent://tenant/namespace/topic
// The short topic name can be:
// - <topic>
// - <property>/<namespace>/<topic>

open TopicNameHelpers

type TopicName private (completeTopicName: string, partition: int) =

    let isPersistent = completeTopicName.StartsWith("persistent")
    let isPartitioned = partition > -1

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
        TopicName(completeTopicName, GetPartitionIndex(completeTopicName))
        
    internal new (domain: string, namespaceName: NamespaceName, topic: string) =
        let name = domain + "://" + namespaceName.ToString() + "/" + topic
        TopicName(name)

    member this.CompleteTopicName: CompleteTopicName = %completeTopicName

    member this.IsPersistent = isPersistent

    member this.IsPartitioned = isPartitioned

    member this.PartitionIndex = partition
    
    member this.NamespaceName =
        let regexMatch = FullTopicRegex.Match(completeTopicName)
        regexMatch.Groups.[2].Value + "/" + regexMatch.Groups.[3].Value
        |> NamespaceName

    member this.GetPartition(index: int) =
        if (index = -1 || isPartitioned) then
            this
        else
            let partitionedTopicName = completeTopicName + PartitionTopicSuffix + index.ToString()
            TopicName(partitionedTopicName, index)
            
    static member TRANSACTION_COORDINATOR_ASSIGN =
        TopicName("persistent", NamespaceName.SYSTEM_NAMESPACE, "transaction_coordinator_assign")

    override this.Equals obj =
        match obj with
        | :? TopicName as t -> completeTopicName.Equals(%t.CompleteTopicName)
        | _ -> false

    override this.GetHashCode () =
        completeTopicName.GetHashCode()
    override this.ToString() =
        %completeTopicName
        
    interface System.IComparable with
        member x.CompareTo tn = completeTopicName.CompareTo(tn)
        
    interface System.IComparable<string> with
        member x.CompareTo tn = completeTopicName.CompareTo(tn)