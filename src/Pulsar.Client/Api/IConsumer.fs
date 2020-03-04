namespace Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common

type IConsumer =

    /// Receive a single message, wait asynchronously if no message is ready.
    abstract member ReceiveAsync: unit -> Task<Message>
    /// Retrieves messages when has enough messages or wait timeout and completes with received messages.
    abstract member BatchReceiveAsync: unit -> Task<Messages>
    /// Asynchronously acknowledge the consumption of a single message
    abstract member AcknowledgeAsync: messageId:MessageId -> Task<unit>
    /// Asynchronously acknowledge the consumption of Messages
    abstract member AcknowledgeAsync: messages:Messages -> Task<unit>
    /// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
    abstract member AcknowledgeCumulativeAsync: messageId:MessageId -> Task<unit>
    /// Redelivers all the unacknowledged messages
    abstract member RedeliverUnacknowledgedMessagesAsync: unit -> Task<unit>
    /// Clean up resources
    abstract member CloseAsync: unit -> Task<unit>
    /// Unsubscribes consumer
    abstract member UnsubscribeAsync: unit -> Task<unit>
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync: messageId:MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (unix timestamp).
    abstract member SeekAsync: timestamp:uint64 -> Task<unit>
    /// Get the last message id available available for consume.
    abstract member GetLastMessageIdAsync: unit -> Task<MessageId>
    /// Acknowledge the failure to process a single message.
    abstract member NegativeAcknowledge: messageId:MessageId -> Task<unit>
    /// Acknowledge the failure to process Messages
    abstract member NegativeAcknowledge: messages:Messages -> Task<unit>
    /// Internal client consumer id
    abstract member ConsumerId: ConsumerId
    /// Get a topic for the consumer
    abstract member Topic: string
    /// Get the consumer name
    abstract member Name: string