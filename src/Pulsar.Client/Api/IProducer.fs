namespace Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common

type IProducer =

    /// Send message and await confirmation from broker
    abstract member SendAsync: message:byte[] -> Task<MessageId>
    /// Send message with keys and props
    abstract member SendAsync: messageBuilder:MessageBuilder -> Task<MessageId>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: message:byte[] -> Task<unit>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: messageBuilder:MessageBuilder -> Task<unit>
    /// Clean up resources
    abstract member CloseAsync: unit -> Task<unit>
    /// Internal client producer id
    abstract member ProducerId: ProducerId
    /// Get the topic which producer is publishing to
    abstract member Topic: string

    /// The last sequence id that was published by this producer.
    /// This represent either the automatically assigned
    /// or custom sequence id that was published and acknowledged by the broker.
    /// After recreating a producer with the same producer name, this will return the last message that was
    /// published in the previous producer session, or -1 if there no message was ever published.
    abstract member LastSequenceId : int64

    /// Get the producer name
    abstract member Name: string