namespace Pulsar.Client.Api

open Pulsar.Client.Common

type MessageId = 
    {
        LedgerId: LedgerId
        EntryId: EntryId
        PartitionIndex: int
    }

type Message =
    {
        MessageId: MessageId
        Payload: byte[]
    }

