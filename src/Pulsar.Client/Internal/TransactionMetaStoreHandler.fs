namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Threading.Tasks
open System.Timers
open Pulsar.Client.Api
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Transaction
open pulsar.proto

type internal TransRequestTime = {
    CreationTime: DateTime
    RequestId: RequestId
}

type internal TransactionMetaStoreMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | NewTransaction of TimeSpan * AsyncReplyChannel<Task<TxnId>>
    | NewTransactionResponse of RequestId * ResultOrException<TxnId>
    | AddPartitionToTxn of TxnId * CompleteTopicName * AsyncReplyChannel<Task<unit>>
    | AddPartitionToTxnResponse of RequestId * ResultOrException<unit>
    | AddSubscriptionToTxn of TxnId * CompleteTopicName * SubscriptionName * AsyncReplyChannel<Task<unit>>
    | AddSubscriptionToTxnResponse of RequestId * ResultOrException<unit>
    | EndTxn of TxnId * TxnAction * AsyncReplyChannel<Task<unit>>
    | EndTxnResponse of RequestId * ResultOrException<unit>
    | TimeoutTick
    | Close

type internal TxnRequest =
    | NewTransaction of TxnId
    | Empty
    static member GetNewTransaction = function
        | NewTransaction x -> x
        | _ -> failwith "Incorrect return type"
        
    static member GetEmpty = function
        | Empty -> ()
        | _ -> failwith "Incorrect return type"

type internal TransactionMetaStoreHandler(clientConfig: PulsarClientConfiguration,
                                    transactionCoordinatorId: TransactionCoordinatorId,
                                    completeTopicName: CompleteTopicName, connectionPool: ConnectionPool,
                                    lookup: BinaryLookupService, transactionCoordinatorCreatedTsc: TaskCompletionSource<unit>) as this =
    let prefix = $"tmsHandler-{transactionCoordinatorId}"
    let mutable operationsLeft = 1000
    let blockIfReachMaxPendingOps = true
    let pendingRequests = Dictionary<RequestId, TaskCompletionSource<TxnRequest>>()
    let blockedRequests = Queue<TransactionMetaStoreMessage>()
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
        NewTxnResponse = fun result -> this.Mb.Post(TransactionMetaStoreMessage.NewTransactionResponse result)
        AddPartitionToTxnResponse = fun result -> this.Mb.Post(TransactionMetaStoreMessage.AddPartitionToTxnResponse result)
        AddSubscriptionToTxnResponse = fun result -> this.Mb.Post(TransactionMetaStoreMessage.AddSubscriptionToTxnResponse result)
        EndTxnResponse = fun result -> this.Mb.Post(TransactionMetaStoreMessage.EndTxnResponse result)
        ConnectionClosed = fun clientCnx -> this.Mb.Post(TransactionMetaStoreMessage.ConnectionClosed clientCnx)
    }

    let startRequest (clientCnx: ClientCnx) requestId msg command =
        if operationsLeft > 0 then
            let tcs = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            clientCnx.SendAndForget command
            pendingRequests.Add(requestId, tcs)
            timeoutQueue.Enqueue({ CreationTime = DateTime.Now; RequestId = requestId })
            tcs.Task
        elif blockIfReachMaxPendingOps then
            let tcs = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            blockedRequests.Enqueue(msg)
            tcs.Task
        else
            MetaStoreHandlerNotReadyException "Reach max pending ops."
            |> Task.FromException<TxnRequest>
    
    let timeoutTimer = new Timer()
    let timeoutException = TimeoutException "Could not get response from transaction meta store within given timeout."
    
    let startTimeoutTimer () =
        timeoutTimer.Interval <- clientConfig.OperationTimeout.TotalMilliseconds
        timeoutTimer.AutoReset <- true
        timeoutTimer.Elapsed.Add(fun _ -> this.Mb.Post(TimeoutTick))
        timeoutTimer.Start()
    
    let rec checkTimeoutedMessages () =
        if timeoutQueue.Count > 0
            && timeoutQueue.Peek().CreationTime + clientConfig.OperationTimeout < DateTime.Now then
                let lastPolled = timeoutQueue.Dequeue()
                match pendingRequests.TryGetValue lastPolled.RequestId with
                | true, op ->
                    op.SetException timeoutException
                    Log.Logger.LogDebug("{0} Transaction coordinator request {1} is timeout.", prefix, lastPolled.RequestId)
                | _ ->
                    ()
                checkTimeoutedMessages()
    
    let mb = MailboxProcessor<TransactionMetaStoreMessage>.Start(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
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
                    
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} New transaction with timeout {1} reqId {2}", prefix, ttl, requestId)
                        let command = Commands.newTxn transactionCoordinatorId requestId ttl
                        task {
                            let! result = startRequest clientCnx requestId msg command
                            return TxnRequest.GetNewTransaction result
                        } |> ch.Reply
                    | _ ->
                        ch.Reply(Task.FromException<TxnId>(NotConnectedException "Not connected"))
                        Log.Logger.LogWarning("{0} is not ready for NewTransaction", prefix)
                    return! loop ()
                    
                | TransactionMetaStoreMessage.NewTransactionResponse (reqId, txnIdResult) ->
                    
                    match pendingRequests.TryGetValue reqId with
                    | true, op ->
                        pendingRequests.Remove(reqId) |> ignore
                        match txnIdResult with
                        | Ok txnId ->
                            Log.Logger.LogDebug("{0} NewTransactionResponse reqId={1} txnId={2}", prefix, reqId, txnId)
                            op.SetResult(NewTransaction txnId)
                        | Error ex ->
                            if ex :? CoordinatorNotFoundException then
                                connectionHandler.ReconnectLater ex
                            Log.Logger.LogError(ex, "{0} NewTransactionResponse reqId={1}", prefix, reqId)
                            op.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got new txn response for timeout reqId={1}, result={2}",
                                              prefix, reqId, txnIdResult.ToStr())
                    return! loop ()
                    
                | TransactionMetaStoreMessage.AddPartitionToTxn (txnId, partition, ch) ->
                    
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} AddPartitionToTxn txnId {1}, partition {2}, reqId {3}",
                                            prefix, txnId, partition, requestId)
                        let command = Commands.newAddPartitionToTxn txnId requestId partition
                        task {
                            let! result = startRequest clientCnx requestId msg command
                            return TxnRequest.GetEmpty result
                        } |> ch.Reply
                    | _ ->
                        ch.Reply(Task.FromException<Unit>(NotConnectedException "Not connected"))
                        Log.Logger.LogWarning("{0} is not ready for AddPartitionToTxn", prefix)
                    return! loop ()
                    
                | TransactionMetaStoreMessage.AddPartitionToTxnResponse (reqId, result) ->
                    
                    match pendingRequests.TryGetValue reqId with
                    | true, op ->
                        pendingRequests.Remove(reqId) |> ignore
                        match result with
                        | Ok () ->
                            Log.Logger.LogDebug("{0} AddPartitionToTxnResponse reqId={1}", prefix, reqId)
                            op.SetResult(Empty)
                        | Error ex ->
                            if ex :? CoordinatorNotFoundException then
                                connectionHandler.ReconnectLater ex
                            Log.Logger.LogError(ex, "{0} AddPartitionToTxnResponse reqId={1}", prefix, reqId)
                            op.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got addPartitionToTxn response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())
                    return! loop ()
                    
                | TransactionMetaStoreMessage.AddSubscriptionToTxn (txnId, topic, subscription, ch) ->
                    
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} AddSubscriptionToTxn txnId {1}, topic {2}, subscription {3}, requestId {4}",
                                            prefix, txnId, topic, subscription, requestId)
                        let command = Commands.newAddSubscriptionToTxn txnId requestId topic subscription
                        task {
                            let! result = startRequest clientCnx requestId msg command
                            return TxnRequest.GetEmpty result
                        } |> ch.Reply
                    | _ ->
                        ch.Reply(Task.FromException<Unit>(NotConnectedException "Not connected"))
                        Log.Logger.LogWarning("{0} is not ready for AddSubscriptionToTxn", prefix)
                    return! loop ()
                    
                | TransactionMetaStoreMessage.AddSubscriptionToTxnResponse (reqId, result) ->
                    
                    match pendingRequests.TryGetValue reqId with
                    | true, op ->
                        pendingRequests.Remove(reqId) |> ignore
                        match result with
                        | Ok () ->
                            Log.Logger.LogDebug("{0} AddSubscriptionToTxnResponse reqId={1}", prefix, reqId)
                            op.SetResult(Empty)
                        | Error ex ->
                            if ex :? CoordinatorNotFoundException then
                                connectionHandler.ReconnectLater ex
                            Log.Logger.LogError(ex, "{0} AddSubscriptionToTxnResponse reqId={1}", prefix, reqId)
                            op.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got addSubscriptionToTxn response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())
                    return! loop ()
                
                | TransactionMetaStoreMessage.EndTxn (txnId, txnAction, ch) ->
                    
                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready clientCnx ->
                        
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} EndTxn txnId {1}, action {2}, requestId {3}",
                                            prefix, txnId, txnAction, requestId)
                        let command = Commands.newEndTxn txnId requestId txnAction
                        task {
                            let! result = startRequest clientCnx requestId msg command
                            return TxnRequest.GetEmpty result
                        } |> ch.Reply
                    | _ ->
                        ch.Reply(Task.FromException<Unit>(NotConnectedException "Not connected"))
                        Log.Logger.LogWarning("{0} is not ready for EndTxn", prefix)
                    return! loop ()
                    
                | TransactionMetaStoreMessage.EndTxnResponse (reqId, result) ->
                    
                    match pendingRequests.TryGetValue reqId with
                    | true, op ->
                        pendingRequests.Remove(reqId) |> ignore
                        match result with
                        | Ok () ->
                            Log.Logger.LogDebug("{0} EndTxnResponse reqId={1}", prefix, reqId)
                            op.SetResult(Empty)
                        | Error ex ->
                            if ex :? CoordinatorNotFoundException then
                                connectionHandler.ReconnectLater ex
                            Log.Logger.LogError(ex, "{0} EndTxnResponse reqId={1}", prefix, reqId)
                            op.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got end transaction response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())
                    return! loop ()
                    
                | TimeoutTick ->
                        
                    checkTimeoutedMessages()
                    return! loop()
                    
                | Close ->
                    
                    timeoutTimer.Stop()
                    for KeyValue(_, v) in pendingRequests do
                        v.SetException(AlreadyClosedException "{0} is closed")
                    pendingRequests.Clear()
                    timeoutQueue.Clear()
                    connectionHandler.Close()
           }
        loop ()
    )
    
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do connectionHandler.GrabCnx()
    do startTimeoutTimer()
    
    member private this.Mb: MailboxProcessor<TransactionMetaStoreMessage> = mb
    
    member this.NewTransactionAsync(ttl: TimeSpan) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        task {
            let! t = mb.PostAndAsyncReply(fun ch -> TransactionMetaStoreMessage.NewTransaction(ttl, ch))
            return! t
        }
        
    member this.AddPublishPartitionToTxnAsync(txnId: TxnId, partition) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        task {
            let! t = mb.PostAndAsyncReply(fun ch -> TransactionMetaStoreMessage.AddPartitionToTxn(txnId, partition, ch))
            return! t
        }
        
    member this.AddSubscriptionToTxnAsync(txnId: TxnId, topic: CompleteTopicName, subscription: SubscriptionName) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        task {
            let! t = mb.PostAndAsyncReply(fun ch ->
                TransactionMetaStoreMessage.AddSubscriptionToTxn(txnId, topic, subscription, ch))
            return! t
        }
        
    member this.EdTxnAsync(txnId: TxnId, txnAction: TxnAction) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        task {
            let! t = mb.PostAndAsyncReply(fun ch ->
                TransactionMetaStoreMessage.EndTxn(txnId, txnAction, ch))
            return! t
        }
        
    member this.Close() =
        mb.Post(Close)