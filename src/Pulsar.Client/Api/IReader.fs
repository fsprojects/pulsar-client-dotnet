namespace Pulsar.Client.Api

open System
open System.Threading
open System.Threading.Tasks
open Pulsar.Client.Common

type IReader<'T> =
    inherit IAsyncDisposable

    // Receive a single message, wait asynchronously if no message is ready.
    abstract member ReadNextAsync: unit -> Task<Message<'T>>
    // Receive a single message, wait asynchronously if no message is ready, can be cancelled.
    abstract member ReadNextAsync: CancellationToken -> Task<Message<'T>>
    /// Reset the subscription associated with this consumer to a specific message id.
    abstract member SeekAsync : messageId:MessageId -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message publish time (Unix timestamp in ms).    
    abstract member SeekAsync : timestamp:TimeStamp -> Task<unit>
    /// Reset the subscription associated with this consumer to a specific message id or publish time (unix timestamp), returned by resolver function.
    abstract member SeekAsync: resolver: Func<string, SeekType> -> Task<unit>    
    /// Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    abstract member HasReachedEndOfTopic: bool
    /// Check if there is any message available to read from the current position.
    abstract member HasMessageAvailableAsync: unit -> Task<bool>
    /// Get a topic for the reader
    abstract member Topic: string
    /// Return true if the reader is connected to the broker
    abstract member IsConnected: bool