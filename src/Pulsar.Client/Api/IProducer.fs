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
    /// <remarks>
    ///     This <paramref name="deliverAt" /> timestamp must be expressed as unix time milliseconds based on UTC.
    ///     For example: <code>DateTimeOffset.UtcNow.AddSeconds(2.0).ToUnixTimeMilliseconds()</code>.
    /// </remarks>
    abstract member NewMessage:
        value:'T
        * [<Optional; DefaultParameterValue(null:string)>]key:string
        * [<Optional; DefaultParameterValue(null:IReadOnlyDictionary<string,string>)>]properties: IReadOnlyDictionary<string, string>
        * [<Optional; DefaultParameterValue(Nullable():Nullable<int64>)>]deliverAt:Nullable<int64>
        -> MessageBuilder<'T>