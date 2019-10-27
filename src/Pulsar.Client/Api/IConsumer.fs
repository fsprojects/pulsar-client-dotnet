namespace Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common

type IConsumer =

    /// Receive a single message, wait asynchronously if no message is ready. Should be only one awaiting thread per consumer.
    abstract member ReceiveAsync: unit -> Task<Message>
    /// Asynchronously acknowledge the consumption of a single message
    abstract member AcknowledgeAsync: MessageId -> Task<unit>
    /// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
    abstract member AcknowledgeCumulativeAsync: MessageId -> Task<unit>
    /// Redelivers all the unacknowledged messages
    abstract member RedeliverUnacknowledgedMessagesAsync: unit -> Task<unit>
    /// Clean up resources
    abstract member CloseAsync: unit -> Task<unit>
    /// Unsubscribes consumer
    abstract member UnsubscribeAsync: unit -> Task<unit>
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync: MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (timestamp).
    abstract member SeekAsync: uint64 -> Task<unit>
    /// Acknowledge the failure to process a single message.
    abstract member NegativeAcknowledge: MessageId -> Task<unit>