namespace Pulsar.Client.Internal

open Pulsar.Client.Common
open System.Threading.Tasks

type ILookupService =
    abstract member GetPartitionedTopicMetadata: string -> Task<PartitionedTopicMetadata>
