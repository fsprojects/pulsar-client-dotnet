namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System.Threading.Tasks

type internal ILookupService =

    //  Get the partitions of the topic.
    abstract member GetPartitionsForTopic: TopicName -> Task<TopicName[]>

    //  Get the partitions if the topic exists. Return "{partition: n}" if a partitioned topic exists;
    //  return "{partition: 0}" if a non-partitioned topic exists.
    abstract member GetPartitionedTopicMetadata: CompleteTopicName -> Task<PartitionedTopicMetadata>

    //  Calls broker lookup-api to get broker which serves namespace bundle that contains given topic.
    abstract member GetBroker: CompleteTopicName -> Task<Broker>

    //  Returns all topics under the given namespace.
    abstract member GetTopicsUnderNamespace: NamespaceName * isPersistent: bool -> Task<string[]>

    //  Returns current SchemaInfo for a given topic.
    abstract member GetSchema: CompleteTopicName * ?schema: SchemaVersion -> Task<TopicSchema option>

    //  Updates the service url.
    abstract member UpdateServiceUrl: ServiceUri -> unit
