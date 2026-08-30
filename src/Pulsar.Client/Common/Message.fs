namespace Pulsar.Client.Common

open System
open System.Collections
open System.Collections.Generic
open Pulsar.Client.Api

type Message<'T> internal (messageId: MessageId, data: byte[], key: PartitionKey, hasBase64EncodedKey: bool,
                  properties: IReadOnlyDictionary<string, string>, encryptionCtx: EncryptionContext option,
                  schemaVersion: byte[], sequenceId: SequenceId, orderingKey: byte[], publishTime: TimeStamp,
                  eventTime: Nullable<TimeStamp>,
                  redeliveryCount: int32, replicatedFrom: string, producerName: string, consumerEpoch: Nullable<ConsumerEpoch>,
                  readerSchema: ISchema<'T> option, getValue: unit -> 'T) =
    /// Get the unique message ID associated with this message.
    member this.MessageId = messageId
    /// Get the raw payload of the message.
    member this.Data = data
    /// Get the key of the message.
    member this.Key = key
    /// Check whether the key has been base64 encoded.
    member this.HasBase64EncodedKey = hasBase64EncodedKey
    /// Return the properties attached to the message.
    member this.Properties = properties
    /// Schema version of the message if the message is produced with schema otherwise null.
    member this.SchemaVersion = schemaVersion
    /// Get the sequence id associated with this message
    member this.SequenceId = sequenceId
    /// EncryptionContext contains encryption and compression information in it using which application can
    /// decrypt consumed message with encrypted-payload.
    member this.EncryptionContext = encryptionCtx
    /// Get the ordering key of the message.
    member this.OrderingKey = orderingKey
    /// Get the publish time of the message as Unix timestamp (automatically set by the client library on produce).
    member this.PublishTime = publishTime
    /// Get the event time of the message as Unix timestamp (manually set by the application on produce).
    member this.EventTime = eventTime
    /// Get the redelivery count of the message
    member this.RedeliveryCount = redeliveryCount
    /// Get name of cluster, from which the message is replicated.
    member this.ReplicatedFrom = replicatedFrom
    /// Get name of producer of the message
    member this.ProducerName = producerName
    /// Get the consumer epoch associated with this message
    member this.ConsumerEpoch = consumerEpoch

    /// Get the de-serialized value of the message, according the configured Schema.
    member this.GetValue() =
        getValue()

    /// Get the schema used to decode this message at its written schema version, if available.
    member this.GetReaderSchema() =
        readerSchema

    member internal this.WithMessageId messageId =
        Message(messageId, data, key, hasBase64EncodedKey, properties, encryptionCtx, schemaVersion, sequenceId,
                orderingKey, publishTime, eventTime, redeliveryCount, replicatedFrom, producerName, consumerEpoch,
                readerSchema, getValue)
    /// Get a new instance of the message with updated data
    member this.WithData data =
        Message(messageId, data, key, hasBase64EncodedKey, properties, encryptionCtx, schemaVersion, sequenceId,
                orderingKey, publishTime, eventTime, redeliveryCount, replicatedFrom, producerName, consumerEpoch,
                readerSchema, getValue)
    /// Get a new instance of the message with updated key
    member this.WithKey (key, hasBase64EncodedKey) =
        Message(messageId, data, key, hasBase64EncodedKey, properties, encryptionCtx, schemaVersion, sequenceId,
                orderingKey, publishTime, eventTime, redeliveryCount, replicatedFrom, producerName, consumerEpoch,
                readerSchema, getValue)
    /// Get a new instance of the message with updated properties
    member this.WithProperties properties =
        Message(messageId, data, key, hasBase64EncodedKey, properties, encryptionCtx, schemaVersion, sequenceId,
                orderingKey, publishTime, eventTime, redeliveryCount, replicatedFrom, producerName, consumerEpoch,
                readerSchema, getValue)

type Messages<'T> internal(maxNumberOfMessages: int, maxSizeOfMessages: int64) =

    let mutable currentNumberOfMessages = 0
    let mutable currentSizeOfMessages = 0L

    let messageList = if maxNumberOfMessages > 0 then ResizeArray<Message<'T>>(maxNumberOfMessages) else ResizeArray<Message<'T>>()

    member this.Count = currentNumberOfMessages
    member this.Size = currentSizeOfMessages

    member internal this.IsFull =
        currentNumberOfMessages = maxNumberOfMessages
        || currentSizeOfMessages = maxSizeOfMessages

    member internal this.CanAdd(message: Message<'T>) =
        ((maxNumberOfMessages > 0 && currentNumberOfMessages + 1 > maxNumberOfMessages)
            || (maxSizeOfMessages > 0L && currentSizeOfMessages + (int64 message.Data.Length) > maxSizeOfMessages))
        |> not

    member internal this.Add(message: Message<'T>) =
        currentNumberOfMessages <- currentNumberOfMessages + 1
        currentSizeOfMessages <- currentSizeOfMessages + (int64 message.Data.Length)
        messageList.Add(message)

    interface IEnumerable<Message<'T>> with
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> IEnumerator
        member this.GetEnumerator() =
            messageList.GetEnumerator() :> IEnumerator<Message<'T>>
