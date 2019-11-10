namespace Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common

type IProducer =

    /// Send message and await confirmation from broker
    abstract member SendAsync: byte[] -> Task<MessageId>
    /// Send message with keys and props
    abstract member SendAsync: MessageBuilder -> Task<MessageId>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: byte[] -> Task<unit>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: MessageBuilder -> Task<unit>
    /// Clean up resources
    abstract member CloseAsync: unit -> Task<unit>
    /// Internal client producer id
    abstract member ProducerId: ProducerId