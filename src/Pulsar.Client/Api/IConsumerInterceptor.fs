namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common

type IConsumerInterceptor<'T> =
    
    abstract member Close: unit -> unit
    
    abstract member BeforeConsume: consumer:IConsumer<'T> * message:Message<'T> -> Message<'T>  

    abstract member OnAcknowledge: consumer:IConsumer<'T> * messageId:MessageId * ``exception``:Exception -> unit

    abstract member OnAcknowledgeCumulative: consumer:IConsumer<'T> * messageId:MessageId * ``exception``:Exception -> unit
    
    abstract member OnNegativeAcksSend: consumer:IConsumer<'T> * messageId:MessageId -> unit
    
    abstract member OnAckTimeoutSend: consumer:IConsumer<'T> * messageId:MessageId -> unit