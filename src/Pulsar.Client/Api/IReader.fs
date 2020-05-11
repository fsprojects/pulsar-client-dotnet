namespace Pulsar.Client.Api

open System
open System.Threading.Tasks
open Pulsar.Client.Common

type IReader<'T> =
    inherit IAsyncDisposable

    // Receive a single message, wait asynchronously if no message is ready.
    abstract member ReadNextAsync: unit -> Task<Message<'T>>
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync : messageId:MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (unix timestamp).    
    abstract member SeekAsync : timestamp:uint64 -> Task<unit>
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Check if there is any message available to read from the current position.
    abstract member HasMessageAvailableAsync: unit -> Task<bool>
    /// Get a topic for the reader
    abstract member Topic: string