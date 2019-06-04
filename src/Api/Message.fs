namespace Pulsar.Client.Api

type MessageId = 
    {
        LedgerId: int64
        EntryId: int64
        PartitionIndex: int;
    }

type Message =
    {
        MessageId: MessageId
    }

