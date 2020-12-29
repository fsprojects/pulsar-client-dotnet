namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Transaction

type TransRequestTime = {
    CreationTime: DateTime
    RequestId: RequestId
}

type internal TransactionMetaStoreMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | NewTransaction of TimeSpan * AsyncReplyChannel<Task<TxnId>>

type internal TransactionMetaStoreHandler(clientConfig: PulsarClientConfiguration,
                                    transactionCoordinatorId: TransactionCoordinatorId,
                                    completeTopicName: CompleteTopicName, connectionPool: ConnectionPool,
                                    lookup: BinaryLookupService, transactionCoordinatorCreatedTsc: TaskCompletionSource<unit>) as this =
    let prefix = $"tmsHandler-{transactionCoordinatorId}"
    let mutable operationsLeft = 1000
    let blockIfReachMaxPendingOps = true
    let pendingRequests = Dictionary<RequestId, TaskCompletionSource<TxnId>>()
    let blockedRequests = Queue<TimeSpan*TaskCompletionSource<TxnId>>()
    let timeoutQueue = Queue<TransRequestTime>()
    
    let connectionHandler = 
        ConnectionHandler(prefix,
                      connectionPool,
                      lookup,
                      completeTopicName,
                      (fun _ -> this.Mb.Post(TransactionMetaStoreMessage.ConnectionOpened)),
                      (fun ex -> this.Mb.Post(TransactionMetaStoreMessage.ConnectionFailed ex)),
                      Backoff({ BackoffConfig.Default with
                                    Initial = clientConfig.InitialBackoffInterval
                                    Max = clientConfig.MaxBackoffInterval }))

    let transactionMetaStoreOperations = {
        ConnectionClosed = fun (clientCnx) -> this.Mb.Post(TransactionMetaStoreMessage.ConnectionClosed clientCnx)
    }

    
    let mb = MailboxProcessor<TransactionMetaStoreMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                match! inbox.Receive() with
                | TransactionMetaStoreMessage.ConnectionOpened ->
                    
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        Log.Logger.LogInformation("{0} connection opened.", prefix)
                        clientCnx.AddTransactionMetaStoreHandler(transactionCoordinatorId, transactionMetaStoreOperations)
                        transactionCoordinatorCreatedTsc.TrySetResult() |> ignore
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready, current state is ", prefix)
                    return! loop ()
                       
                | TransactionMetaStoreMessage.ConnectionFailed ex ->
                    
                    Log.Logger.LogError(ex, "{0} connection failed.", prefix)
                    connectionHandler.Failed()
                    transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore
                    return! loop ()
                    
                | TransactionMetaStoreMessage.ConnectionClosed clientCnx ->
                    
                    Log.Logger.LogDebug("{0} connection closed", prefix)
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveTransactionMetaStoreHandler(transactionCoordinatorId)
                    return! loop ()
                    
                | TransactionMetaStoreMessage.NewTransaction (ttl, ch) ->
                    
                    Log.Logger.LogDebug("{0} New transaction with timeout {1}", prefix, ttl)
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        if operationsLeft > 0 then
                            let tcs = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
                            let requestId = Generators.getNextRequestId()
                            let command = Commands.newTxn transactionCoordinatorId requestId ttl
                            clientCnx.SendAndForget command
                            pendingRequests.Add(requestId, tcs)
                            timeoutQueue.Enqueue({ CreationTime = DateTime.Now; RequestId = requestId })
                            ch.Reply tcs.Task
                        elif blockIfReachMaxPendingOps then
                            let tcs = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
                            blockedRequests.Enqueue(ttl, tcs)
                            ch.Reply tcs.Task
                        else
                            MetaStoreHandlerNotReadyException "Reach max pending ops."
                            |> Task.FromException<TxnId>
                            |> ch.Reply
                    | _ ->
                        Log.Logger.LogWarning("{0} is not ready", prefix)
                    return! loop ()
           }
        loop ()
    )
    
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do connectionHandler.GrabCnx()
    
    member private this.Mb: MailboxProcessor<TransactionMetaStoreMessage> = mb
    
    member this.NewTransactionAsync(ttl: TimeSpan) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        task {
            let! t = mb.PostAndAsyncReply(fun ch -> NewTransaction(ttl, ch))
            return! t
        }
        