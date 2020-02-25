namespace Pulsar.Client.Api

open Pulsar.Client.Common

type IMessageRouter =
    abstract member ChoosePartition: messageKey:MessageKey * numPartitions:int -> int