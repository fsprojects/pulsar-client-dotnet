module Pulsar.Client.UnitTests.Internal.TransactionMetaStoreHandlerTests

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.Transaction
open Pulsar.Client.UnitTests.Internal.LocalBroker
open Pulsar.Client.UnitTests.Internal.ProducerImplTests

let private clientConfig =
    { PulsarClientConfiguration.Default with
        OperationTimeout = TimeSpan.FromMilliseconds(250.0)
        StatsInterval = TimeSpan.FromMinutes(1.0)
        KeepAliveInterval = TimeSpan.FromMinutes(1.0) }

let private topic: CompleteTopicName = %"persistent://pulsar/system/transaction_coordinator_assign"

let private answerNewTransaction (requestId: uint64) (send: BaseCommand -> unit) (leastBits: uint64) =
    send (BaseCommand(
            ``type`` = BaseCommand.Type.NewTxnResponse,
            newTxnResponse =
                CommandNewTxnResponse(RequestId = requestId, TxnidLeastBits = leastBits, TxnidMostBits = 0UL)))

/// A broker that answers the handler's connect to the coordinator, fails the attempts it is told to fail with an
/// error that can be retried, and leaves the new transactions to the test. It reports each attempt it has answered.
let private coordinatorBroker (failedAttempts: int list) =
    let attempts = Channel.CreateUnbounded<int>()
    let newTransactions = Channel.CreateUnbounded<uint64 * (BaseCommand -> unit)>()
    let mutable attempt = 0
    let broker =
        new LocalBroker(fun command send ->
            match command.``type`` with
            | BaseCommand.Type.TcClientConnectRequest ->
                let number = Interlocked.Increment(&attempt)
                let response = CommandTcClientConnectResponse(RequestId = command.tcClientConnectRequest.RequestId)
                if List.contains number failedAttempts then
                    response.Error <- ServerError.ServiceNotReady
                    response.Message <- "The coordinator is not ready"
                send (BaseCommand(``type`` = BaseCommand.Type.TcClientConnectResponse, tcClientConnectResponse = response))
                attempts.Writer.TryWrite number |> ignore
            | BaseCommand.Type.NewTxn ->
                newTransactions.Writer.TryWrite((command.newTxn.RequestId, send)) |> ignore
            | _ ->
                ())
    struct (broker, attempts.Reader, newTransactions.Reader)

let private nextOf (reader: ChannelReader<'T>) =
    reader.ReadAsync().AsTask().WaitAsync(testTimeout)

[<Tests>]
let tests =
    testList "TransactionMetaStoreHandler" [
        testTask "An answer that comes after its request has timed out doesn't stop the handler" {
            // the broker answers the handler's connect, and leaves the new transactions to the test
            let newTransactions = Channel.CreateUnbounded<uint64 * (BaseCommand -> unit)>()
            let broker =
                new LocalBroker(fun command send ->
                    match command.``type`` with
                    | BaseCommand.Type.TcClientConnectRequest ->
                        send (BaseCommand(
                                ``type`` = BaseCommand.Type.TcClientConnectResponse,
                                tcClientConnectResponse =
                                    CommandTcClientConnectResponse(RequestId = command.tcClientConnectRequest.RequestId)))
                    | BaseCommand.Type.NewTxn ->
                        newTransactions.Writer.TryWrite((command.newTxn.RequestId, send)) |> ignore
                    | _ ->
                        ())
            let answer (requestId: uint64) (send: BaseCommand -> unit) (leastBits: uint64) =
                send (BaseCommand(
                        ``type`` = BaseCommand.Type.NewTxnResponse,
                        newTxnResponse =
                            CommandNewTxnResponse(RequestId = requestId, TxnidLeastBits = leastBits, TxnidMostBits = 0UL)))
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)

                // the broker takes longer than the operation timeout, and answers after all
                let timedOut = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (requestId: uint64, send: BaseCommand -> unit) =
                    newTransactions.Reader.ReadAsync().AsTask().WaitAsync(testTimeout)
                let! _ = expectFailure<Pulsar.Client.Api.TimeoutException> timedOut
                answer requestId send 1UL

                // as in the Java client, that answer is one for an unknown request, and the handler goes on
                let next = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (nextRequestId: uint64, nextSend: BaseCommand -> unit) =
                    newTransactions.Reader.ReadAsync().AsTask().WaitAsync(testTimeout)
                answer nextRequestId nextSend 2UL
                let! (txnId: TxnId) = next.WaitAsync(testTimeout)
                Expect.equal txnId.LeastSigBits 2UL "The next transaction gets its own answer"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A closed transaction coordinator client answers a start" {
            let broker = new LocalBroker()
            let connectionPool = ConnectionPool(clientConfig)
            try
                let client = TransactionCoordinatorClient(clientConfig, connectionPool, lookupFor broker)
                client.Close()

                // the client's builder waits for this answer
                let! (result: ResultOrException<unit>) = client.Start().WaitAsync(TimeSpan.FromSeconds(5.0))
                match result with
                | Error (:? CoordinatorClientStateException) -> ()
                | other -> failtestf "Expected the start to be refused, got %A" other
            finally
                (broker :> IDisposable).Dispose()
        }

        testTask "A handler that is being created connects to the coordinator again after a failed attempt" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, attempts, _) = coordinatorBroker [ 1 ]
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                // as in the Java client, an error that can be retried doesn't end the creation
                do! created.Task.WaitAsync(testTimeout)
                let! (first: int) = nextOf attempts
                let! (second: int) = nextOf attempts
                Expect.equal (first, second) (1, 2) "The handler was created by its second attempt"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A created handler connects to the coordinator again after a failed attempt" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, attempts, newTransactions) = coordinatorBroker [ 2 ]
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)
                let! (_: int) = nextOf attempts

                // the broker goes away, and when it is back its coordinator is not ready at first
                broker.DropConnections()
                let! (_: int) = nextOf attempts
                // as in the Java client, instead of staying on a connection that doesn't know the handler
                let! (retried: int) = nextOf attempts
                Expect.equal retried 3 "The handler connected to the coordinator again"

                // the connection knows the handler again, so its transactions get their answers
                let pending = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (requestId: uint64, send: BaseCommand -> unit) = nextOf newTransactions
                answerNewTransaction requestId send 1UL
                let! (txnId: TxnId) = pending.WaitAsync(testTimeout)
                Expect.equal txnId.LeastSigBits 1UL "The transaction is created"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A request waiting for its answer is sent again when the handler has connected again" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, _, newTransactions) = coordinatorBroker []
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)
                let pending = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (requestId: uint64, _: BaseCommand -> unit) = nextOf newTransactions

                // the broker goes away before it has answered
                broker.DropConnections()

                // as in the Java client, instead of the request waiting for its timeout
                let! (resentId: uint64, send: BaseCommand -> unit) = nextOf newTransactions
                Expect.equal resentId requestId "The same request is sent again"
                answerNewTransaction resentId send 1UL
                let! (txnId: TxnId) = pending.WaitAsync(testTimeout)
                Expect.equal txnId.LeastSigBits 1UL "The request gets its answer"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A request made while the handler connects is sent once it has connected" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, attempts, newTransactions) = coordinatorBroker [ 2 ]
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)
                let! (_: int) = nextOf attempts

                // the broker goes away, and its coordinator is not ready when the handler comes back
                broker.DropConnections()
                let! (_: int) = nextOf attempts

                // as in the Java client, a request made before the next attempt waits for it instead of failing
                let pending = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (requestId: uint64, send: BaseCommand -> unit) = nextOf newTransactions
                answerNewTransaction requestId send 1UL
                let! (txnId: TxnId) = pending.WaitAsync(testTimeout)
                Expect.equal txnId.LeastSigBits 1UL "The request gets its answer"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        testTask "A request the coordinator was not found for is sent again" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, _, newTransactions) = coordinatorBroker []
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)
                let pending = handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0))
                let! (requestId: uint64, send: BaseCommand -> unit) = nextOf newTransactions

                // the broker no longer has the coordinator
                send (BaseCommand(
                        ``type`` = BaseCommand.Type.NewTxnResponse,
                        newTxnResponse =
                            CommandNewTxnResponse(
                                RequestId = requestId,
                                TxnidMostBits = 0UL,
                                Error = ServerError.TransactionCoordinatorNotFound,
                                Message = "The coordinator has moved")))

                // as in the Java client, the handler connects again and sends the request again, instead of failing it
                let! (resentId: uint64, resend: BaseCommand -> unit) = nextOf newTransactions
                Expect.equal resentId requestId "The same request is sent again"
                answerNewTransaction resentId resend 1UL
                let! (txnId: TxnId) = pending.WaitAsync(testTimeout)
                Expect.equal txnId.LeastSigBits 1UL "The request gets its answer"
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }

        // sequenced: its thousand requests would slow down the tests that depend on timing
        testSequenced <| testTask "No more than 1000 requests wait for their answers at once" {
            let clientConfig = { clientConfig with OperationTimeout = TimeSpan.FromSeconds(30.0) }
            let struct (broker, _, newTransactions) = coordinatorBroker []
            let connectionPool = ConnectionPool(clientConfig)
            let created = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler = TransactionMetaStoreHandler(clientConfig, %0UL, topic, connectionPool, lookupFor broker, created)
            try
                do! created.Task.WaitAsync(testTimeout)
                let limit = 1000
                let requests = Array.init (limit + 1) (fun _ -> handler.NewTransactionAsync(TimeSpan.FromMinutes(1.0)))

                // as in the Java client, the broker gets no more than the limit
                let received = ResizeArray<uint64 * (BaseCommand -> unit)>()
                for _ in 1 .. limit do
                    let! (request: uint64 * (BaseCommand -> unit)) = nextOf newTransactions
                    received.Add request
                do! Task.Delay(300)
                Expect.equal newTransactions.Count 0 "The request over the limit waits for one of the others to finish"

                // one answer lets it through
                let (firstId, firstSend) = received[0]
                answerNewTransaction firstId firstSend 1UL
                let! (lastId: uint64, lastSend: BaseCommand -> unit) = nextOf newTransactions
                answerNewTransaction lastId lastSend 1UL

                for (requestId, send) in received |> Seq.skip 1 do
                    answerNewTransaction requestId send 1UL
                let! (_: TxnId[]) = Task.WhenAll(requests).WaitAsync(testTimeout)
                return ()
            finally
                handler.Close()
                connectionPool.CloseAllConnections().Wait(testTimeout) |> ignore
                (broker :> IDisposable).Dispose()
        }
    ]
