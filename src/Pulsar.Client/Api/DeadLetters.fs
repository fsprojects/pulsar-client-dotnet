namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System.Runtime.InteropServices

type DeadLettersPolicy(maxRedeliveryCount : int, [<Optional; DefaultParameterValue(null : string)>] deadLetterTopic : string) =
    member __.MaxRedeliveryCount = maxRedeliveryCount
    member __.DeadLetterTopic = deadLetterTopic

type IDeadLettersProcessor =
    abstract member ClearMessages: unit -> unit
    abstract member AddMessage: MessageId -> Message -> unit
    abstract member RemoveMessage: MessageId -> unit
    abstract member ProcessMessages: MessageId -> bool