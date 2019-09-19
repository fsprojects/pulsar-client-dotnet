namespace Pulsar.Client.Api

type IMessageRouter =
    abstract member ChoosePartition: int -> int