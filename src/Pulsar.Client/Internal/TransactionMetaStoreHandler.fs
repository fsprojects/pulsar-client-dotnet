namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Threading.Tasks
open System.Timers
open Pulsar.Client.Api
open Pulsar.Client.Common
open Microsoft.Extensions.Logging

open Pulsar.Client.Transaction
open pulsar.proto
open System.Threading.Channels

type internal TransRequestTime = {
    CreationTime: DateTime
    RequestId: RequestId
}

type internal TransactionMetaStoreMessage =
    | ConnectionOpened
    | ConnectionFailed of exn
    | ConnectionClosed of ClientCnx
    | NewTransaction of TimeSpan * TaskCompletionSource<TxnId>
    | NewTransactionResponse of RequestId * ResultOrException<TxnId>
    | AddPartitionToTxn of TxnId * CompleteTopicName * TaskCompletionSource<unit>
    | AddPartitionToTxnResponse of RequestId * ResultOrException<unit>
    | AddSubscriptionToTxn of TxnId * CompleteTopicName * SubscriptionName * TaskCompletionSource<unit>
    | AddSubscriptionToTxnResponse of RequestId * ResultOrException<unit>
    | EndTxn of TxnId * TxnAction * TaskCompletionSource<unit>
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
                      (fun _ -> post this.Mb TransactionMetaStoreMessage.ConnectionOpened),
                      (fun ex -> post this.Mb (TransactionMetaStoreMessage.ConnectionFailed ex)),
                      Backoff({ BackoffConfig.Default with
                                    Initial = clientConfig.InitialBackoffInterval
                                    Max = clientConfig.MaxBackoffInterval }))

    let transactionMetaStoreOperations = {
        NewTxnResponse = fun result -> post this.Mb (TransactionMetaStoreMessage.NewTransactionResponse result)
        AddPartitionToTxnResponse = fun result -> post this.Mb (TransactionMetaStoreMessage.AddPartitionToTxnResponse result)
        AddSubscriptionToTxnResponse = fun result -> post this.Mb (TransactionMetaStoreMessage.AddSubscriptionToTxnResponse result)
        EndTxnResponse = fun result -> post this.Mb (TransactionMetaStoreMessage.EndTxnResponse result)
        ConnectionClosed = fun clientCnx -> post this.Mb (TransactionMetaStoreMessage.ConnectionClosed clientCnx)
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
        timeoutTimer.Elapsed.Add(fun _ -> post this.Mb TimeoutTick)
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

    let mb = Channel.CreateUnbounded<TransactionMetaStoreMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            match msg with
            | TransactionMetaStoreMessage.ConnectionOpened ->

                match connectionHandler.ConnectionState with
                | ConnectionState.Ready clientCnx ->
                    Log.Logger.LogInformation("{0} connection opened.", prefix)
                    if clientCnx.RemoteEndpointProtocolVersion > ProtocolVersion.V18 then
                        try
                            let requestId = Generators.getNextRequestId()
                            let payload = Commands.newTcClientConnectRequest transactionCoordinatorId requestId
                            let! response = clientCnx.SendAndWaitForReply requestId payload
                            response |> PulsarResponseType.GetEmpty
                            Log.Logger.LogInformation("{0} Transaction coordinator client connect success!", transactionCoordinatorId)
                            clientCnx.AddTransactionMetaStoreHandler(transactionCoordinatorId, transactionMetaStoreOperations)
                            connectionHandler.ResetBackoff()
                            transactionCoordinatorCreatedTsc.TrySetResult() |> ignore
                        with Flatten ex ->
                            Log.Logger.LogError(ex, "{0} Transaction coordinator client connect fail!", transactionCoordinatorId)
                            transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore
                    else
                        clientCnx.AddTransactionMetaStoreHandler(transactionCoordinatorId, transactionMetaStoreOperations)
                        transactionCoordinatorCreatedTsc.TrySetResult() |> ignore
                | _ ->
                    Log.Logger.LogWarning("{0} connection opened but connection is not ready, current state is ", prefix)

            | TransactionMetaStoreMessage.ConnectionFailed ex ->

                Log.Logger.LogError(ex, "{0} connection failed.", prefix)
                connectionHandler.Failed()
                transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore

            | TransactionMetaStoreMessage.ConnectionClosed clientCnx ->

                Log.Logger.LogDebug("{0} connection closed", prefix)
                connectionHandler.ConnectionClosed clientCnx
                clientCnx.RemoveTransactionMetaStoreHandler(transactionCoordinatorId)

            | TransactionMetaStoreMessage.NewTransaction (ttl, channel) ->

                match connectionHandler.ConnectionState with
                | ConnectionState.Ready clientCnx ->

                    let requestId = Generators.getNextRequestId()
                    Log.Logger.LogDebug("{0} New transaction with timeout {1} reqId {2}", prefix, ttl, requestId)
                    let command = Commands.newTxn transactionCoordinatorId requestId ttl
                    backgroundTask {
                        try
                            let! result = startRequest clientCnx requestId msg command
                            TxnRequest.GetNewTransaction result |> channel.SetResult
                        with Flatten ex ->
                            channel.SetException ex
                    } |> ignore
                | _ ->
                    channel.SetException(NotConnectedException "Not connected")
                    Log.Logger.LogWarning("{0} is not ready for NewTransaction", prefix)

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

            | TransactionMetaStoreMessage.AddPartitionToTxn (txnId, partition, channel) ->

                match connectionHandler.ConnectionState with
                | ConnectionState.Ready clientCnx ->

                    let requestId = Generators.getNextRequestId()
                    Log.Logger.LogDebug("{0} AddPartitionToTxn txnId {1}, partition {2}, reqId {3}",
                                        prefix, txnId, partition, requestId)
                    let command = Commands.newAddPartitionToTxn txnId requestId partition
                    backgroundTask {
                        try
                            let! result = startRequest clientCnx requestId msg command
                            TxnRequest.GetEmpty result |> channel.SetResult
                        with Flatten ex ->
                            channel.SetException ex
                    }  |> ignore
                | _ ->
                    channel.SetException(NotConnectedException "Not connected")
                    Log.Logger.LogWarning("{0} is not ready for AddPartitionToTxn", prefix)

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

            | TransactionMetaStoreMessage.AddSubscriptionToTxn (txnId, topic, subscription, channel) ->

                match connectionHandler.ConnectionState with
                | ConnectionState.Ready clientCnx ->

                    let requestId = Generators.getNextRequestId()
                    Log.Logger.LogDebug("{0} AddSubscriptionToTxn txnId {1}, topic {2}, subscription {3}, requestId {4}",
                                        prefix, txnId, topic, subscription, requestId)
                    let command = Commands.newAddSubscriptionToTxn txnId requestId topic subscription

                    backgroundTask {
                        try
                            let! result = startRequest clientCnx requestId msg command
                            TxnRequest.GetEmpty result |> channel.SetResult
                        with Flatten ex ->
                            channel.SetException ex
                    }  |> ignore
                | _ ->
                    channel.SetException(NotConnectedException "Not connected")
                    Log.Logger.LogWarning("{0} is not ready for AddSubscriptionToTxn", prefix)

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

            | TransactionMetaStoreMessage.EndTxn (txnId, txnAction, channel) ->

                match connectionHandler.ConnectionState with
                | ConnectionState.Ready clientCnx ->

                    let requestId = Generators.getNextRequestId()
                    Log.Logger.LogDebug("{0} EndTxn txnId {1}, action {2}, requestId {3}",
                                        prefix, txnId, txnAction, requestId)
                    let command = Commands.newEndTxn txnId requestId txnAction
                    backgroundTask {
                        try
                            let! result = startRequest clientCnx requestId msg command
                            TxnRequest.GetEmpty result |> channel.SetResult
                        with Flatten ex ->
                            channel.SetException ex
                    }  |> ignore
                | _ ->
                    channel.SetException(NotConnectedException "Not connected")
                    Log.Logger.LogWarning("{0} is not ready for EndTxn", prefix)

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

            | TimeoutTick ->

                checkTimeoutedMessages()

            | Close ->

                timeoutTimer.Stop()
                for KeyValue(_, v) in pendingRequests do
                    v.SetException(AlreadyClosedException "{0} is closed")
                pendingRequests.Clear()
                timeoutQueue.Clear()
                connectionHandler.Close()
                continueLoop <- false

        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    do connectionHandler.GrabCnx()
    do startTimeoutTimer()

    member private this.Mb: Channel<TransactionMetaStoreMessage> = mb

    member this.NewTransactionAsync(ttl: TimeSpan) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        postAndAsyncReply mb (fun ch -> TransactionMetaStoreMessage.NewTransaction(ttl, ch))

    member this.AddPublishPartitionToTxnAsync(txnId: TxnId, partition) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        postAndAsyncReply mb (fun ch -> TransactionMetaStoreMessage.AddPartitionToTxn(txnId, partition, ch))

    member this.AddSubscriptionToTxnAsync(txnId: TxnId, topic: CompleteTopicName, subscription: SubscriptionName) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        postAndAsyncReply mb (fun ch ->
                TransactionMetaStoreMessage.AddSubscriptionToTxn(txnId, topic, subscription, ch))

    member this.EdTxnAsync(txnId: TxnId, txnAction: TxnAction) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        postAndAsyncReply mb (fun ch ->
                TransactionMetaStoreMessage.EndTxn(txnId, txnAction, ch))

    member this.Close() =
        post mb Close