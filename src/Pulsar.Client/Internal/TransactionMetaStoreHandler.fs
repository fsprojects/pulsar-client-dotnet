namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading.Tasks
open System.Timers
open Pulsar.Client.Api
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open FSharp.UMX

open Pulsar.Client.Transaction
open pulsar.proto
open System.Threading.Channels

type internal TransRequestTime = {
    CreationTime: TimeStamp
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
    | RetryRequest of RequestId
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

/// A request to the coordinator that has not been answered yet, with what it takes to send it again.
type internal PendingTxnRequest =
    {
        Command: SendTask
        Callback: TaskCompletionSource<TxnRequest>
        // between the attempts for a request the coordinator was not found for
        Backoff: Backoff
    }

type internal TransactionMetaStoreHandler(clientConfig: PulsarClientConfiguration,
                                    transactionCoordinatorId: TransactionCoordinatorId,
                                    completeTopicName: CompleteTopicName, connectionPool: ConnectionPool,
                                    lookup: ILookupService, transactionCoordinatorCreatedTsc: TaskCompletionSource<unit>) as this =
    let prefix = $"tmsHandler-{transactionCoordinatorId}"
    // As in the Java client, no more than this many requests wait for their answers at once.
    let requestPermits = new System.Threading.SemaphoreSlim(1000)
    let pendingRequests = Dictionary<RequestId, PendingTxnRequest>()
    let timeoutQueue = Queue<TransRequestTime>()
    let createTmsHandlerStartTime = Stopwatch.GetTimestamp()

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

    // As in the Java client, a request stays pending until it is answered or has timed out, whatever happens to the
    // connection in between. It is sent if the handler is connected, and waits while the handler connects, which
    // sends it once it has connected. Returns whether the request is still pending.
    let sendRequest (request: PendingTxnRequest) =
        match connectionHandler.ConnectionState with
        | ConnectionState.Ready clientCnx ->
            clientCnx.SendAndForget request.Command
            true
        | ConnectionState.Connecting
        | ConnectionState.Uninitialized ->
            true
        | ConnectionState.Closing
        | ConnectionState.Closed ->
            request.Callback.TrySetException(AlreadyClosedException $"{prefix} is closed") |> ignore
            false
        | _ ->
            request.Callback.TrySetException(NotConnectedException "Not connected") |> ignore
            false

    let resendPendingRequests () =
        let failed = ResizeArray<RequestId>()
        for KeyValue(requestId, request) in pendingRequests do
            if not (sendRequest request) then
                failed.Add requestId
        for requestId in failed do
            pendingRequests.Remove(requestId) |> ignore

    // As in the Java client, a request the coordinator was not found for doesn't fail. The handler connects again,
    // and the request is sent again, after its backoff or once the handler has connected, until it times out.
    // Returns whether the request is pending again.
    let retryLater (requestId: RequestId) (request: PendingTxnRequest) (ex: exn) =
        match ex with
        | :? CoordinatorNotFoundException ->
            match connectionHandler.ConnectionState with
            | ConnectionState.Connecting -> ()
            | _ -> connectionHandler.ReconnectLater ex
            pendingRequests[requestId] <- request
            asyncDelayMs (request.Backoff.Next()) (fun () -> post this.Mb (TransactionMetaStoreMessage.RetryRequest requestId))
            true
        | _ ->
            false

    let startRequest requestId command =
        let request =
            {
                Command = command
                Callback = TaskCompletionSource<TxnRequest>(TaskCreationOptions.RunContinuationsAsynchronously)
                Backoff = Backoff({ BackoffConfig.Default with
                                        Initial = clientConfig.InitialBackoffInterval
                                        Max = clientConfig.MaxBackoffInterval / 10.0 })
            }
        pendingRequests.Add(requestId, request)
        timeoutQueue.Enqueue({ CreationTime = %Stopwatch.GetTimestamp(); RequestId = requestId })
        if not (sendRequest request) then
            pendingRequests.Remove(requestId) |> ignore
        request.Callback.Task

    let timeoutTimer = new Timer()
    let timeoutException = TimeoutException "Could not get response from transaction meta store within given timeout."

    let startTimeoutTimer () =
        timeoutTimer.Interval <- clientConfig.OperationTimeout.TotalMilliseconds
        timeoutTimer.AutoReset <- true
        timeoutTimer.Elapsed.Add(fun _ -> post this.Mb TimeoutTick)
        timeoutTimer.Start()

    let rec checkTimeoutedMessages () =
        if timeoutQueue.Count > 0
            && Stopwatch.GetElapsedTime(%timeoutQueue.Peek().CreationTime) > clientConfig.OperationTimeout then
                let lastPolled = timeoutQueue.Dequeue()
                // As in the Java client, the request is no longer pending: a response that still comes is one for
                // an unknown request, not one that completes this request a second time.
                let mutable op = Unchecked.defaultof<PendingTxnRequest>
                if pendingRequests.Remove(lastPolled.RequestId, &op) then
                    op.Callback.TrySetException timeoutException |> ignore
                    Log.Logger.LogDebug("{0} Transaction coordinator request {1} is timeout.", prefix, lastPolled.RequestId)
                checkTimeoutedMessages()

    let stopTransactionMetaStoreHandler() =
        timeoutTimer.Stop()
        for KeyValue(_, v) in pendingRequests do
            v.Callback.TrySetException(AlreadyClosedException $"{prefix} is closed") |> ignore
        pendingRequests.Clear()
        timeoutQueue.Clear()
        connectionHandler.Close()
        Log.Logger.LogInformation("{0} stopped", prefix)

    // As in the Java client, a failure to connect ends the creation of the handler if its error can't be retried, or
    // once the creation has taken longer than the operation timeout. Returns whether it stopped the handler.
    let failsCreation (ex: exn) =
        let nonRetriableError = ex |> PulsarClientException.isRetriableError |> not
        let timeout = Stopwatch.GetElapsedTime(createTmsHandlerStartTime) > clientConfig.OperationTimeout
        if ((nonRetriableError || timeout) && transactionCoordinatorCreatedTsc.TrySetException(ex)) then
            Log.Logger.LogInformation("{0} connection failed {1}", prefix,
                                        if nonRetriableError then "with unretriableError" else "after timeout")
            connectionHandler.Failed(ex)
            stopTransactionMetaStoreHandler()
            true
        else
            false

    let mb = Channel.CreateUnbounded<TransactionMetaStoreMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))

    // for a request the loop no longer answers: the handler is closed, or it has failed
    let stoppedException () =
        match connectionHandler.ConnectionState with
        | ConnectionState.Closing
        | ConnectionState.Closed ->
            AlreadyClosedException $"{prefix} is closed" :> exn
        | _ ->
            NotConnectedException($"{prefix} not connected", connectionHandler.FailureCause) :> exn

    let failRequest (ex: exn) (msg: TransactionMetaStoreMessage) =
        match msg with
        | TransactionMetaStoreMessage.NewTransaction (_, channel) -> channel.TrySetException ex |> ignore
        | TransactionMetaStoreMessage.AddPartitionToTxn (_, _, channel) -> channel.TrySetException ex |> ignore
        | TransactionMetaStoreMessage.AddSubscriptionToTxn (_, _, _, channel) -> channel.TrySetException ex |> ignore
        | TransactionMetaStoreMessage.EndTxn (_, _, channel) -> channel.TrySetException ex |> ignore
        | _ -> ()

    do (backgroundTask {
        let mutable continueLoop = true
        while continueLoop do
            let! msg = mb.Reader.ReadAsync()
            try
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
                                resendPendingRequests()
                            with Flatten ex ->
                                Log.Logger.LogError(ex, "{0} Transaction coordinator client connect fail!", transactionCoordinatorId)
                                // As in the Java client. A client that is not allowed to connect to the coordinator
                                // closes the handler, and its connection. After any other failure the handler connects
                                // again, unless the failure ends its creation. Without that it would stay on a
                                // connection that doesn't know it, and no transaction would get an answer.
                                if ex :? NotAllowedException then
                                    connectionHandler.Closed()
                                    clientCnx.Dispose()
                                    transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore
                                    stopTransactionMetaStoreHandler()
                                    continueLoop <- false
                                elif failsCreation ex then
                                    continueLoop <- false
                                else
                                    connectionHandler.ReconnectLater ex
                        else
                            clientCnx.AddTransactionMetaStoreHandler(transactionCoordinatorId, transactionMetaStoreOperations)
                            transactionCoordinatorCreatedTsc.TrySetResult() |> ignore
                            // the Java client leaves them to time out on a broker this old
                            resendPendingRequests()
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready, current state is ", prefix)

                | TransactionMetaStoreMessage.ConnectionFailed ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if connectionHandler.ConnectionState = ConnectionState.Failed then
                        // Only the connection handler itself fails while this handler runs: its loop has stopped, and
                        // nothing would connect this handler again.
                        Log.Logger.LogError(ex, "{0} stopping because its connection handler failed", prefix)
                        transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore
                        stopTransactionMetaStoreHandler()
                        continueLoop <- false
                    elif failsCreation ex then
                        continueLoop <- false

                | TransactionMetaStoreMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} connection closed", prefix)
                    connectionHandler.ConnectionClosed clientCnx

                | TransactionMetaStoreMessage.NewTransaction (ttl, channel) ->

                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready _
                    | ConnectionState.Connecting
                    | ConnectionState.Uninitialized ->

                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} New transaction with timeout {1} reqId {2}", prefix, ttl, requestId)
                        let command = Commands.newTxn transactionCoordinatorId requestId ttl
                        backgroundTask {
                            try
                                let! result = startRequest requestId command
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
                            op.Callback.SetResult(NewTransaction txnId)
                        | Error ex ->
                            if not (retryLater reqId op ex) then
                                Log.Logger.LogError(ex, "{0} NewTransactionResponse reqId={1}", prefix, reqId)
                                op.Callback.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got new txn response for timeout reqId={1}, result={2}",
                                              prefix, reqId, txnIdResult.ToStr())

                | TransactionMetaStoreMessage.AddPartitionToTxn (txnId, partition, channel) ->

                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready _
                    | ConnectionState.Connecting
                    | ConnectionState.Uninitialized ->

                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} AddPartitionToTxn txnId {1}, partition {2}, reqId {3}",
                                            prefix, txnId, partition, requestId)
                        let command = Commands.newAddPartitionToTxn txnId requestId partition
                        backgroundTask {
                            try
                                let! result = startRequest requestId command
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
                            op.Callback.SetResult(Empty)
                        | Error ex ->
                            if not (retryLater reqId op ex) then
                                Log.Logger.LogError(ex, "{0} AddPartitionToTxnResponse reqId={1}", prefix, reqId)
                                op.Callback.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got addPartitionToTxn response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())

                | TransactionMetaStoreMessage.AddSubscriptionToTxn (txnId, topic, subscription, channel) ->

                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready _
                    | ConnectionState.Connecting
                    | ConnectionState.Uninitialized ->

                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} AddSubscriptionToTxn txnId {1}, topic {2}, subscription {3}, requestId {4}",
                                            prefix, txnId, topic, subscription, requestId)
                        let command = Commands.newAddSubscriptionToTxn txnId requestId topic subscription

                        backgroundTask {
                            try
                                let! result = startRequest requestId command
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
                            op.Callback.SetResult(Empty)
                        | Error ex ->
                            if not (retryLater reqId op ex) then
                                Log.Logger.LogError(ex, "{0} AddSubscriptionToTxnResponse reqId={1}", prefix, reqId)
                                op.Callback.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got addSubscriptionToTxn response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())

                | TransactionMetaStoreMessage.EndTxn (txnId, txnAction, channel) ->

                    match connectionHandler.ConnectionState with
                    | ConnectionState.Ready _
                    | ConnectionState.Connecting
                    | ConnectionState.Uninitialized ->

                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogDebug("{0} EndTxn txnId {1}, action {2}, requestId {3}",
                                            prefix, txnId, txnAction, requestId)
                        let command = Commands.newEndTxn txnId requestId txnAction
                        backgroundTask {
                            try
                                let! result = startRequest requestId command
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
                            op.Callback.SetResult(Empty)
                        | Error ex ->
                            if not (retryLater reqId op ex) then
                                Log.Logger.LogError(ex, "{0} EndTxnResponse reqId={1}", prefix, reqId)
                                op.Callback.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} Got end transaction response for timeout reqId={1} result={2}",
                                                prefix, reqId, result.ToStr())

                | TransactionMetaStoreMessage.RetryRequest reqId ->

                    match pendingRequests.TryGetValue reqId with
                    | true, request ->
                        if not (sendRequest request) then
                            pendingRequests.Remove(reqId) |> ignore
                    | _ ->
                        Log.Logger.LogDebug("{0} The request {1} has already timed out", prefix, reqId)

                | TimeoutTick ->

                    checkTimeoutedMessages()

                | Close ->

                    connectionHandler.Closed()
                    stopTransactionMetaStoreHandler()
                    continueLoop <- false

            // As in the Java client, where the handler is not a loop that an exception could stop: only the request
            // that failed is lost, and later transactions still get their answers.
            with Flatten ex ->
                Log.Logger.LogError(ex, "{0} failed to process a request", prefix)
                failRequest ex msg
                match msg with
                | Close -> continueLoop <- false
                | _ -> ()

        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                connectionHandler.Failed(ex)
                // the transaction coordinator client waits for this while it starts
                transactionCoordinatorCreatedTsc.TrySetException(ex) |> ignore
            // Nothing reads the mailbox any more: the requests still queued are refused here, and later ones by
            // their caller, instead of being left waiting.
            mb.Writer.TryComplete() |> ignore
            let mutable msg = Unchecked.defaultof<TransactionMetaStoreMessage>
            while mb.Reader.TryRead(&msg) do
                failRequest (stoppedException ()) msg
            if t.IsFaulted then
                stopTransactionMetaStoreHandler()
                Log.Logger.LogCritical(connectionHandler.FailureCause, "{0} mailbox failure", prefix)
            else
                Log.Logger.LogInformation("{0} mailbox has stopped normally", prefix))
    |> ignore

    // As in the Java client, a request takes a permit before it is made, and gives it back once it has its answer,
    // its failure or its timeout. A request that finds no permit waits for one; the Java client blocks the calling
    // thread for that.
    let requestWithPermit (message: TaskCompletionSource<'R> -> TransactionMetaStoreMessage) : Task<'R> =
        backgroundTask {
            do! requestPermits.WaitAsync()
            try
                return! postAndAsyncReplyOrFail mb stoppedException message
            finally
                requestPermits.Release() |> ignore
        }

    do connectionHandler.GrabCnx()
    do startTimeoutTimer()

    member private this.Mb: Channel<TransactionMetaStoreMessage> = mb

    member this.NewTransactionAsync(ttl: TimeSpan) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        requestWithPermit (fun ch -> TransactionMetaStoreMessage.NewTransaction(ttl, ch))

    member this.AddPublishPartitionToTxnAsync(txnId: TxnId, partition) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        requestWithPermit (fun ch -> TransactionMetaStoreMessage.AddPartitionToTxn(txnId, partition, ch))

    member this.AddSubscriptionToTxnAsync(txnId: TxnId, topic: CompleteTopicName, subscription: SubscriptionName) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        requestWithPermit (fun ch ->
                TransactionMetaStoreMessage.AddSubscriptionToTxn(txnId, topic, subscription, ch))

    member this.EdTxnAsync(txnId: TxnId, txnAction: TxnAction) =
        connectionHandler.CheckIfActive() |> throwIfNotNull
        requestWithPermit (fun ch ->
                TransactionMetaStoreMessage.EndTxn(txnId, txnAction, ch))

    member this.Close() =
        post mb Close