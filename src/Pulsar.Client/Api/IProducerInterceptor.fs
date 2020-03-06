namespace Pulsar.Client.Api

open System
open System.Runtime.InteropServices
open Pulsar.Client.Common

type IProducerInterceptor =
    
    abstract member Close: unit -> unit
    
    abstract member Eligible: message:MessageBuilder -> bool
    
    abstract member BeforeSend: producer:IProducer * message:MessageBuilder -> MessageBuilder  

    abstract member OnSendAcknowledgement: producer:IProducer * message:MessageBuilder * messageId:MessageId * ``exception``:Exception -> unit
