namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common

type IConsumerInterceptor =
    
    abstract member Close: unit -> unit
    
    abstract member BeforeConsume: consumer:IConsumer * message:Message -> Message  

    abstract member OnAcknowledge: consumer:IConsumer * messageId:MessageId * ``exception``:Exception -> unit

    abstract member OnAcknowledgeCumulative: consumer:IConsumer * messageId:MessageId * ``exception``:Exception -> unit
    
    abstract member OnNegativeAcksSend: consumer:IConsumer * messageId:MessageId * ``exception``:Exception -> unit
    
    abstract member OnAckTimeoutSend: consumer:IConsumer * messageId:MessageId * ``exception``:Exception -> unit