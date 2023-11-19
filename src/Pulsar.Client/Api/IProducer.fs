namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Threading.Tasks
open Pulsar.Client.Common
open System.Runtime.InteropServices
open Pulsar.Client.Transaction

type IProducer<'T> =
    inherit IAsyncDisposable

    /// Send message and await confirmation from broker
    abstract member SendAsync: message:'T -> Task<MessageId>
    /// Send message with keys and props
    abstract member SendAsync: messageBuilder:MessageBuilder<'T> -> Task<MessageId>
    /// Complete as soon as message reaches internal message queue, respect backpressure, but don't await broker confirmation
    abstract member SendAndForgetAsync: message:'T -> Task<unit>
    /// Complete as soon as message reaches internal message queue, respect backpressure, but don't await broker confirmation
    abstract member SendAndForgetAsync: messageBuilder:MessageBuilder<'T> -> Task<unit>
    /// Internal client producer id
    abstract member ProducerId: ProducerId
    /// Get the topic which producer is publishing to
    abstract member Topic: string    
    /// Get statistics for the producer.
    abstract member GetStatsAsync: unit -> Task<ProducerStats>
    /// <summary>
    ///     Constructs <see cref="Pulsar.Client.Common.MessageBuilder" />
    /// </summary>
    /// <param name="value">Message data</param>
    /// <param name="key">Key of the message for routing policy.</param>
    /// <param name="properties">The readonly dictionary with message properties.</param>
    /// <param name="deliverAt">Unix timestamp in milliseconds after which message should be delivered to consumer(s).timestamp must be expressed as unix time milliseconds based on UTC.
    ///     For example: <code>DateTimeOffset.UtcNow.AddSeconds(2.0).ToUnixTimeMilliseconds()</code></param>
    /// <param name="sequenceId">
    ///     Specify a custom sequence id for the message being published.
    ///     The sequence id can be used for deduplication purposes and it needs to follow these rules:
    ///         - <c>sequenceId >= 0</c>
    ///         - Sequence id for a message needs to be greater than sequence id for earlier messages:
    ///             <c>sequenceId(N+1) > sequenceId(N)</c>
    ///         - It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
    ///             <c>sequenceId</c> could represent an offset or a cumulative size.
    /// </param>
    /// <param name="keyBytes">Bytes of the key of the message for routing policy.</param>
    /// <param name="orderingKey">Ordering key of the message for message dispatch in Key_Shared mode.</param>
    /// <param name="eventTime">Time of the event set by application (Unix timestamp in ms).</param>
    /// <param name="txn">
    ///     Transaction associated with this message.
    ///     After the transaction commit, it will be made visible to consumer.
    ///     After the transaction abort, it will never be visible to consumer.
    /// </param>
    /// <param name="replicationClusters">Geo-replication clusters for this message.
    ///     Set this value to MessageBuilder.DisableReplication to disable replication for the message.</param>
    abstract member NewMessage:
        value:'T
        * [<Optional; DefaultParameterValue(null:string)>]key:string
        * [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string, string>)>]properties: IReadOnlyDictionary<string, string>
        * [<Optional; DefaultParameterValue(Nullable():Nullable<TimeStamp>)>]deliverAt:Nullable<TimeStamp>
        * [<Optional; DefaultParameterValue(Nullable():Nullable<SequenceId>)>]sequenceId:Nullable<SequenceId>
        * [<Optional; DefaultParameterValue(null:byte[])>]keyBytes:byte[]
        * [<Optional; DefaultParameterValue(null:byte[])>]orderingKey:byte[]
        * [<Optional; DefaultParameterValue(Nullable():Nullable<TimeStamp>)>]eventTime:Nullable<TimeStamp>
        * [<Optional; DefaultParameterValue(null:Transaction)>]txn:Transaction
        * [<Optional; DefaultParameterValue(null:IEnumerable<string>)>]replicationClusters:IEnumerable<string>
        -> MessageBuilder<'T>
    /// The last sequence id that was published by this producer.
    /// This represent either the automatically assigned
    /// or custom sequence id that was published and acknowledged by the broker.
    /// After recreating a producer with the same producer name, this will return the last message that was
    /// published in the previous producer session, or -1 if there no message was ever published.
    abstract member LastSequenceId : SequenceId
    /// Get the producer name
    abstract member Name: string
    /// The last disconnected timestamp of the producer
    abstract member LastDisconnectedTimestamp: TimeStamp
    /// Return true if the consumer is connected to the broker
    abstract member IsConnected: bool
