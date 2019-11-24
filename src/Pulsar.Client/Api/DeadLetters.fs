namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System.Runtime.InteropServices
open System.Threading.Tasks

type DeadLettersPolicy(maxRedeliveryCount : int, [<Optional; DefaultParameterValue(null : string)>] deadLetterTopic : string) =
    member __.MaxRedeliveryCount = maxRedeliveryCount
    member __.DeadLetterTopic = deadLetterTopic

type IDeadLettersProcessor =
    abstract member ClearMessages: unit -> unit
    abstract member AddMessage: MessageId -> ResizeArray<Message> -> unit
    abstract member RemoveMessage: MessageId -> unit
    abstract member ProcessMessages: MessageId -> (MessageId -> Async<unit>) -> Task<bool>
    abstract member MaxRedeliveryCount: uint32