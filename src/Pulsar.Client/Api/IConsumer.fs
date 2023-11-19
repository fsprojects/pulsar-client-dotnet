namespace Pulsar.Client.Api

open System
open System.Threading
open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.Transaction

type IConsumer<'T> =
    inherit IAsyncDisposable

    /// Receive a single message, wait asynchronously if no message is ready.
    abstract member ReceiveAsync: unit -> Task<Message<'T>>
    /// Receive a single message, wait asynchronously if no message is ready, can be cancelled.
    abstract member ReceiveAsync: CancellationToken -> Task<Message<'T>>
    /// Retrieves messages when has enough messages or wait timeout and completes with received messages.
    abstract member BatchReceiveAsync: unit -> Task<Messages<'T>>
    /// Retrieves messages when has enough messages or wait timeout and completes with received messages, can be cancelled.
    abstract member BatchReceiveAsync: CancellationToken -> Task<Messages<'T>>
    /// Asynchronously acknowledge the consumption of a single message
    abstract member AcknowledgeAsync: messageId:MessageId -> Task<unit>
    /// Asynchronously acknowledge the consumption of a single message, it will store in pending ack. 
    /// After the transaction commit, the message will actually ack.
    /// After the transaction abort, the message will be redelivered.
    abstract member AcknowledgeAsync: messageId:MessageId * txn:Transaction -> Task<unit>
    /// Asynchronously acknowledge the consumption of Messages
    abstract member AcknowledgeAsync: messages:Messages<'T> -> Task<unit>
    /// Asynchronously acknowledge the consumption of Messages
    abstract member AcknowledgeAsync: messages:MessageId seq -> Task<unit>
    /// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
    abstract member AcknowledgeCumulativeAsync: messageId:MessageId -> Task<unit>
    /// Acknowledge the reception of all the messages in the stream up to (and including) the provided message with this
    /// transaction, it will store in transaction pending ack.
    /// After the transaction commit, the end of previous transaction acked message until this transaction
    /// acked message will actually ack.
    /// After the transaction abort, the end of previous transaction acked message until this transaction
    /// acked message will be redelivered to this consumer.
    /// Cumulative acknowledge with transaction only support cumulative ack and now have not support individual and
    /// cumulative ack sharing.
    /// If cumulative ack with a transaction success, we can cumulative ack messageId with the same transaction
    /// more than previous messageId.
    /// It will not be allowed to cumulative ack with a transaction different from the previous one when the previous
    /// transaction haven't commit or abort.
    /// Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
    abstract member AcknowledgeCumulativeAsync: messageId:MessageId * txn:Transaction -> Task<unit>
    /// Redelivers all the unacknowledged messages
    abstract member RedeliverUnacknowledgedMessagesAsync: unit -> Task<unit>
    /// Unsubscribes consumer
    abstract member UnsubscribeAsync: unit -> Task<unit>
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync: messageId:MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (unix timestamp).
    abstract member SeekAsync: timestamp:TimeStamp -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message ID or publish time (unix timestamp), returned by resolver function.
    abstract member SeekAsync: resolver: Func<string, SeekType> -> Task<unit>
    /// Get the last message id available for consume.
    abstract member GetLastMessageIdAsync: unit -> Task<MessageId>
    /// Acknowledge the failure to process a single message.
    abstract member NegativeAcknowledge: messageId:MessageId -> Task<unit>
    /// Acknowledge the failure to process Messages
    abstract member NegativeAcknowledge: messages:Messages<'T> -> Task<unit>
    /// Internal client consumer id
    abstract member ConsumerId: ConsumerId
    /// Get a topic for the consumer
    abstract member Topic: string
    /// Get the consumer name
    abstract member Name: string
    /// Get statistics for the consumer.
    abstract member GetStatsAsync: unit -> Task<ConsumerStats>
    /// ReconsumeLater the consumption of Message
    abstract member ReconsumeLaterAsync: message:Message<'T> * deliverAt:TimeStamp -> Task<unit>
    /// ReconsumeLater the consumption of Messages
    abstract member ReconsumeLaterAsync: messages:Messages<'T> * deliverAt:TimeStamp -> Task<unit>
    /// ReconsumeLater the reception of all the messages in the stream up to (and including) the provided message.
    abstract member ReconsumeLaterCumulativeAsync: message:Message<'T> * deliverAt:TimeStamp -> Task<unit>
    /// The last disconnected timestamp of the consumer    abstract member LastDisconnected: DateTime
    abstract member LastDisconnectedTimestamp: TimeStamp
    /// Return true if the consumer is connected to the broker
    abstract member IsConnected: bool