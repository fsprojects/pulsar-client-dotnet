namespace Pulsar.Client.Api

open System
open System.Collections.Generic
open System.Threading.Tasks
open Pulsar.Client.Common
open System.Runtime.InteropServices

type IProducer<'T> =
    inherit IAsyncDisposable

    /// Send message and await confirmation from broker
    abstract member SendAsync: message:'T -> Task<MessageId>
    /// Send message with keys and props
    abstract member SendAsync: messageBuilder:MessageBuilder<'T> -> Task<MessageId>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: message:'T -> Task<unit>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: messageBuilder:MessageBuilder<'T> -> Task<unit>
    /// Internal client producer id
    abstract member ProducerId: ProducerId
    /// Get the topic which producer is publishing to
    abstract member Topic: string    
    /// <summary>
    ///     Constructs <see cref="Pulsar.Client.Common.MessageBuilder" />
    /// </summary>
    /// <param name="value">Message data</param>
    /// <param name="properties">The readonly dictionary with message properties.</param>
    /// <param name="deliverAt">Unix timestamp in milliseconds after which message should be delivered to consumer(s).</param>
    /// <param name="sequenceId">
    ///     Specify a custom sequence id for the message being published.
    ///     The sequence id can be used for deduplication purposes and it needs to follow these rules:
    ///         - <c>sequenceId >= 0</c>
    ///         - Sequence id for a message needs to be greater than sequence id for earlier messages:
    ///             <c>sequenceId(N+1) > sequenceId(N)</c>
    ///         - It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
    ///             <c>sequenceId</c> could represent an offset or a cumulative size.
    /// </param>    
    /// <remarks>
    ///     This <paramref name="deliverAt" /> timestamp must be expressed as unix time milliseconds based on UTC.
    ///     For example: <code>DateTimeOffset.UtcNow.AddSeconds(2.0).ToUnixTimeMilliseconds()</code>.
    /// </remarks>
    abstract member NewMessage:
        value:'T
        * [<Optional; DefaultParameterValue(null:string)>]key:string
        * [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string,string>)>]properties: IReadOnlyDictionary<string, string>
        * [<Optional; DefaultParameterValue(Nullable():Nullable<int64>)>]deliverAt:Nullable<int64>
        * [<Optional; DefaultParameterValue(Nullable():Nullable<uint64>)>]sequenceId:Nullable<uint64>
        -> MessageBuilder<'T>
    /// The last sequence id that was published by this producer.
    /// This represent either the automatically assigned
    /// or custom sequence id that was published and acknowledged by the broker.
    /// After recreating a producer with the same producer name, this will return the last message that was
    /// published in the previous producer session, or -1 if there no message was ever published.
    abstract member LastSequenceId : int64
    /// Get the producer name
    abstract member Name: string