namespace Pulsar.Client.Api

open System

/// Batch message container for individual messages being published until they are batched and sent to broker.
type IBatchMessageContainer =

    /// Clear the message batch container.
    abstract member Clear : unit -> unit

    /// Check the message batch container is empty.
    abstract member IsEmpty : bool with get

    /// Get count of messages in the message batch container.
    abstract member NumMessagesInBatch : int with get

    /// Get current message batch size of the message batch container in bytes.
    abstract member CurrentBatchSize : int with get

    /// Release the payload and clear the container.
    abstract member Discard : Exception -> unit

    /// Return the batch container batch message in multiple batches
    abstract member IsMultiBatches : bool with get


/// Batcher builder.
type IBatcherBuilder =

    /// Creates Default batch message container.
    abstract member Default : IBatchMessageContainer with get

    /// Key based batch message container.
    abstract member KeyBased : IBatchMessageContainer with get

    abstract member Build : unit -> IBatchMessageContainer


type internal DefaultBatchMessageContainer =

    new() = DefaultBatchMessageContainer()

    interface IBatchMessageContainer with

        member __.Clear() = raise(NotImplementedException())

        member __.IsEmpty with get() = raise(NotImplementedException())

        member __.NumMessagesInBatch with get() = raise(NotImplementedException())

        member __.CurrentBatchSize with get() = raise(NotImplementedException())

        member __.Discard exn = raise(NotImplementedException())

        member __.IsMultiBatches with get() = raise(NotImplementedException())


type internal KeyBasedBatchMessageContainer =

    new() = KeyBasedBatchMessageContainer()

    interface IBatchMessageContainer with

        member __.Clear() = raise(NotImplementedException())

        member __.IsEmpty with get() = raise(NotImplementedException())

        member __.NumMessagesInBatch with get() = raise(NotImplementedException())

        member __.CurrentBatchSize with get() = raise(NotImplementedException())

        member __.Discard exn = raise(NotImplementedException())

        member __.IsMultiBatches with get() = raise(NotImplementedException())


type internal BatcherBuilder(defaultContainer : IBatchMessageContainer, keyBasedContainer : IBatchMessageContainer) =

    new() = BatcherBuilder(DefaultBatchMessageContainer(), KeyBasedBatchMessageContainer())

    interface IBatcherBuilder with

        member val Default = defaultContainer with get

        member val KeyBased = keyBasedContainer with get

        member __.Build() = raise(NotImplementedException())