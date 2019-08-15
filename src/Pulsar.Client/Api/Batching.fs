namespace Pulsar.Client.Api

open System

type BatchMessageContainerConfiguration =
    {
        NumMessagesPerBatch : int
    }
    static member Default =
        {
            NumMessagesPerBatch = 1
        }

/// Batch message container for individual messages being published until they are batched and sent to broker.
type IBatchMessageContainer = interface end


type internal DefaultBatchMessageContainer(configuration : BatchMessageContainerConfiguration) =

    interface IBatchMessageContainer