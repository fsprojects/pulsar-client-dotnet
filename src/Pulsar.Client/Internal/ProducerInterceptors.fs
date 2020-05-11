namespace Pulsar.Client.Internal

open System
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal ProducerInterceptors<'T>(interceptors: IProducerInterceptor<'T> array) =
     member this.Interceptors = interceptors
     static member Empty with get() = ProducerInterceptors<'T>([||])
     
     member this.BeforeSend (producer: IProducer<'T>, message: MessageBuilder<'T>) =
          let mutable interceptorMessage = message         
          for interceptor in interceptors do
               if interceptor.Eligible message then
                    try
                         interceptorMessage <- interceptor.BeforeSend(producer, interceptorMessage)
                    with e ->
                         Log.Logger.LogWarning("Error executing interceptor beforeSend callback topic: {0}", producer.Topic, e)
          interceptorMessage
        
     member this.OnSendAcknowledgement (producer: IProducer<'T>, message: MessageBuilder<'T>, msgId: MessageId, exn: Exception) =
          for interceptor in interceptors do
               if interceptor.Eligible message then
                    try
                         interceptor.OnSendAcknowledgement(producer, message, msgId, exn)
                    with e ->
                         Log.Logger.LogWarning("Error executing interceptor onSendAcknowledgement callback topic: {0}", producer.Topic, e);
          
     member this.Close() =
          for interceptor in interceptors do
               try
                    interceptor.Close()
               with e ->
                    Log.Logger.LogWarning("Fail to close producer interceptor", e);