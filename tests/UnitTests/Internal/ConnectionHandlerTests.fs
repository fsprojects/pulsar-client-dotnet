module Pulsar.Client.UnitTests.Internal.ConnectionHandlerTests

open System
open System.Net
open System.Threading
open System.Threading.Tasks
open Expecto
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal

/// Fails on the log messages the predicate picks, like anything else that can fail where a failure is being dealt
/// with, such as running out of memory. A test that installs it runs alone, because everything shares the logger.
type internal FailingLogger(inner: ILogger, fails: string -> bool, ?failure: unit -> exn) =
    let failure = defaultArg failure (fun () -> InvalidOperationException "could not log" :> exn)
    interface ILogger with
        member _.BeginScope<'TState>(state: 'TState) : IDisposable = inner.BeginScope<'TState>(state)
        member _.IsEnabled(logLevel: LogLevel) = inner.IsEnabled(logLevel)
        member _.Log<'TState>(logLevel: LogLevel, eventId: EventId, state: 'TState, ex: exn,
                              formatter: Func<'TState, exn, string>) =
            if fails (formatter.Invoke(state, ex)) then
                raise (failure ())
            inner.Log<'TState>(logLevel, eventId, state, ex, formatter)

/// Fails with an OperationCanceledException, like a call that times out in something a loop runs. Such an exception
/// leaves the task of the loop it escapes cancelled instead of faulted.
let internal cancellingLogger (inner: ILogger) (fails: string -> bool) =
    FailingLogger(inner, fails, fun () -> OperationCanceledException "the call was cancelled" :> exn)

let private topic = TopicName("public/default/connection-handler").CompleteTopicName

let private broker =
    let endpoint = DnsEndPoint("localhost", 6650)
    { LogicalAddress = LogicalAddress endpoint; PhysicalAddress = PhysicalAddress endpoint }

let private backoff () =
    Backoff({ BackoffConfig.Default with Initial = TimeSpan.FromMilliseconds(10.0); Max = TimeSpan.FromMilliseconds(10.0) })

let private getConnection (_: Broker * int) = Task.FromResult Unchecked.defaultof<ClientCnx>

/// The first lookup fails, and with it the first attempt to connect.
let private lookupFailingOnce () =
    let mutable calls = 0
    { new ILookupService with
        member _.GetBroker _ =
            if Interlocked.Increment(&calls) = 1 then
                Task.FromException<Broker>(Exception "lookup failed")
            else
                Task.FromResult broker
        member _.GetPartitionsForTopic _ = failwith "Unexpected partition lookup"
        member _.GetPartitionedTopicMetadata _ = failwith "Unexpected metadata lookup"
        member _.GetTopicsUnderNamespace(_, _) = failwith "Unexpected namespace lookup"
        member _.GetSchema(_, ?schema) = failwith "Unexpected schema lookup"
        member _.UpdateServiceInfo _ = failwith "Unexpected service update"
        member _.Dispose() = () }

[<Tests>]
let tests =
    testList "ConnectionHandler" [
        testTask "Another attempt follows whatever fails in an attempt to connect" {
            let opened = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            let handler =
                ConnectionHandler("handler-test", getConnection, lookupFailingOnce (), topic,
                                  (fun _ -> opened.TrySetResult() |> ignore),
                                  (fun _ -> failwith "the owner could not be told"),
                                  backoff ())
            handler.GrabCnx()

            // as in the Java client, instead of the handler stopping and its owner staying disconnected
            do! opened.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
            let connected =
                match handler.ConnectionState with
                | Ready _ -> true
                | _ -> false
            Expect.isTrue connected "The handler is connected"
            handler.Close()
        }

        testSequenced <| testTask "The owner is told when the handler stops" {
            let original = Log.Logger
            let failed = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
            try
                Log.Logger <- FailingLogger(original, fun message -> message.Contains "stopping-handler")
                let handler =
                    ConnectionHandler("stopping-handler", getConnection, lookupFailingOnce (), topic,
                                      ignore, (fun ex -> failed.TrySetResult ex |> ignore), backoff ())
                // the attempt fails, and so does everything that deals with that
                handler.GrabCnx()

                // nothing connects the owner again, so it must not wait for that
                let! (cause: exn) = failed.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                match handler.CheckIfActive() with
                | :? NotConnectedException as ex ->
                    Expect.isTrue (obj.ReferenceEquals(ex.InnerException, cause)) "Requests are refused with the cause"
                | ex ->
                    failtestf "Expected NotConnectedException, got %O" ex
            finally
                Log.Logger <- original
        }

        testSequenced <| testTask "The owner is told when a cancellation stops the handler" {
            let original = Log.Logger
            let failed = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
            try
                Log.Logger <- cancellingLogger original (fun message -> message.Contains "cancelled-handler")
                let handler =
                    ConnectionHandler("cancelled-handler", getConnection, lookupFailingOnce (), topic,
                                      ignore, (fun ex -> failed.TrySetResult ex |> ignore), backoff ())
                handler.GrabCnx()

                // a loop that ends cancelled has stopped as much as one that ends faulted, and the owner must not wait
                let! (cause: exn) = failed.Task.WaitAsync(TimeSpan.FromSeconds(5.0))
                Expect.isTrue (cause.InnerException :? OperationCanceledException) "The cause holds the cancellation"
                match handler.CheckIfActive() with
                | :? NotConnectedException as ex ->
                    Expect.isTrue (obj.ReferenceEquals(ex.InnerException, cause)) "Requests are refused with the cause"
                | ex ->
                    failtestf "Expected NotConnectedException, got %O" ex
            finally
                Log.Logger <- original
        }
    ]
