﻿namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common
open System.Runtime.InteropServices
open System.Threading.Tasks

type DeadLetterPolicy(maxRedeliveryCount: int
                       , [<Optional; DefaultParameterValue(null:string)>] deadLetterTopic: string
                       , [<Optional; DefaultParameterValue(null:string)>] retryLetterTopic: string
                       , [<Optional; DefaultParameterValue(null:string)>] initialSubscriptionName: string
                       ) =
    member __.MaxRedeliveryCount = maxRedeliveryCount
    member __.DeadLetterTopic = deadLetterTopic
    member __.RetryLetterTopic = retryLetterTopic
    member __.InitialSubscriptionName = initialSubscriptionName

type IDeadLetterProcessor<'T> =
    abstract member ClearMessages: unit -> unit
    abstract member AddMessage: MessageId * Message<'T> -> unit
    abstract member RemoveMessage: MessageId -> unit
    abstract member ProcessMessage: MessageId * (MessageId -> unit) -> Task<Task<bool>>
    abstract member MaxRedeliveryCount: int
    abstract member TopicName: string
    abstract member ReconsumeLater: Message<'T> * TimeStamp * (MessageId -> unit) -> Task<unit>