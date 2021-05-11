namespace Pulsar.Client.Internal

open System
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal ConsumerInterceptors<'T>(interceptors: IConsumerInterceptor<'T> array) =
     member this.Interceptors = interceptors
     static member Empty with get() = ConsumerInterceptors<'T>([||])
     
     member this.BeforeConsume (consumer: IConsumer<'T>, msg: Message<'T>) =
          let mutable interceptorMessage = msg         
          for interceptor in interceptors do
               try
                    interceptorMessage <- interceptor.BeforeConsume(consumer, interceptorMessage)
               with e ->
                    Log.Logger.LogWarning(e, "Error executing interceptor beforeConsume callback topic: {0} consumerId: {1}",
                                          consumer.Topic, consumer.ConsumerId)
          interceptorMessage
        
     member this.OnAcknowledge (consumer: IConsumer<'T>, msgId: MessageId, exn: Exception) =
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledge(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning(e, "Error executing interceptor OnAcknowledge callback");
                    
     member this.OnAcknowledgeCumulative (consumer: IConsumer<'T>, msgId: MessageId, exn: Exception)=
          for interceptor in interceptors do
               try
                    interceptor.OnAcknowledgeCumulative(consumer, msgId, exn)
               with e ->
                    Log.Logger.LogWarning(e, "Error executing interceptor OnAcknowledgeCumulative callback");

     member this.OnNegativeAcksSend (consumer: IConsumer<'T>, msgIdSet: MessageId seq) =
          for msgId in msgIdSet do
               for interceptor in interceptors do
                    try
                         interceptor.OnNegativeAcksSend(consumer, msgId)
                    with e ->
                         Log.Logger.LogWarning(e, "Error executing interceptor OnNegativeAcksSend callback");
          
     member this.OnAckTimeoutSend (consumer: IConsumer<'T>, msgIdSet: MessageId seq)  =
          for msgId in msgIdSet do
               for interceptor in interceptors do
                    try
                         interceptor.OnAckTimeoutSend(consumer, msgId)
                    with e ->
                         Log.Logger.LogWarning(e, "Error executing interceptor OnAckTimeoutSend callback");
          
     member this.Close() =
          for interceptor in interceptors do
               try
                    interceptor.Dispose()
               with e ->
                    Log.Logger.LogWarning(e, "Fail to close consumer interceptor");