namespace Pulsar.Client.Api

open Pulsar.Client.Common

type MessageId = 
    {
        LedgerId: int64<ledgerId>
        EntryId: int64<entryId>
        PartitionIndex: int
    }

type Message =
    {
        MessageId: MessageId
    }

