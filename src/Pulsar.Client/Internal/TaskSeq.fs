namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Threading.Tasks

open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open System.Threading.Channels

type internal TaskGenerator<'T> = unit -> Task<'T>

type internal TaskSeqMessage<'T> =
    | WhenAnyTask of TaskCompletionSource<Task<'T>>
    | Next of TaskCompletionSource<Task<'T>>
    | NextComplete of Task<'T>
    | AddGenerators of TaskGenerator<'T> seq
    | RemoveGenerator of TaskGenerator<'T> * TaskCompletionSource<unit>

type internal TaskSeq<'T> (initialGenerators: TaskGenerator<'T> seq) =
    let tasks = ResizeArray<Task<'T>>()
    let generators = ResizeArray()
    let mutable started = false
    let mutable nextWaiting = false
    let waitingQueue = Queue()
    let mutable resetWhenAnyTcs = TaskCompletionSource<'T>()
    let random = Random()

    let resetWhenAny () =
        let previous = resetWhenAnyTcs
        resetWhenAnyTcs <- TaskCompletionSource<'T>()
        tasks[0] <- resetWhenAnyTcs.Task
        previous.SetCanceled()

    let whenAnyTask () =
        let tasksCount = tasks.Count
        let randIndex = random.Next(0, tasksCount)
        let mutable i = 0
        let mutable completedFound = false
        let mutable result = null
        while (i < tasksCount && completedFound = false) do
            let index = (randIndex + i) % tasksCount
            let t = tasks[index]
            if t.IsCompleted then
                completedFound <- true
                result <- t
            else
                i <- i + 1
        if completedFound then
            result |> Task.FromResult
        else
            tasks |> Task.WhenAny

    let whenAnyTaskToChannel (channel: TaskCompletionSource<Task<'T>>) =
        backgroundTask {
            try
                let! result = whenAnyTask()
                channel.SetResult result
            with Flatten ex ->
                channel.SetException ex
        } |> ignore

    let mb = Channel.CreateUnbounded<TaskSeqMessage<'T>>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))

    let mutable currentMessage : TaskSeqMessage<'T> option = None
    let mutable stoppedError : exn = null

    let stoppedException () =
        InvalidOperationException($"TaskSeq has stopped and can't produce more results: {stoppedError.Message}", stoppedError) :> exn

    let replyFromStoppedLoop (msg: TaskSeqMessage<'T>) =
        match msg with
        | WhenAnyTask channel
        | Next channel ->
            channel.TrySetException(stoppedException ()) |> ignore
        | RemoveGenerator (_, channel) ->
            // nothing polls a stopped sequence, so there is nothing left to remove the generator from
            channel.TrySetResult() |> ignore
        | NextComplete _
        | AddGenerators _ ->
            ()

    // requests posted after the loop exits would otherwise never be completed
    let drainStoppedLoop () =
        backgroundTask {
            while true do
                let! msg = mb.Reader.ReadAsync()
                try
                    replyFromStoppedLoop msg
                with ex ->
                    Log.Logger.LogWarning(ex, "TaskSeq failed to reply to a request after the loop stopped")
        } |> ignore

    do (backgroundTask {
        while true do
            let! msg = mb.Reader.ReadAsync()
            currentMessage <- Some msg
            match msg with
            | WhenAnyTask channel ->

                Log.Logger.LogTrace("TaskSeq.WhenAnyTask nextWaiting:{0}", nextWaiting)
                whenAnyTaskToChannel channel

            | Next channel ->

                Log.Logger.LogTrace("TaskSeq.Next nextWaiting:{0}", nextWaiting)
                if not started then
                    for i in [1..generators.Count- 1] do
                        generators[i]() |> tasks.Add
                    started <- true
                if nextWaiting || tasks.Count = 1 then
                    waitingQueue.Enqueue channel
                else
                    nextWaiting <- true
                    whenAnyTaskToChannel channel

            | NextComplete completedTask ->

                Log.Logger.LogTrace("TaskSeq.NextComplete nextWaiting:{0}", nextWaiting)
                let index = tasks.IndexOf(completedTask)
                if index > 0 then
                    tasks[index] <- generators[index]()
                else
                    Log.Logger.LogWarning("TaskSeq: generator was removed, but task has completed")
                if tasks.Count > 1 && waitingQueue.Count > 0 then
                    let channel = waitingQueue.Dequeue()
                    whenAnyTaskToChannel channel
                else
                    nextWaiting <- false

            | AddGenerators newGenerators ->

                Log.Logger.LogTrace("TaskSeq.AddGenerators nextWaiting:{0}", nextWaiting)
                let noGenerators = tasks.Count = 1
                newGenerators |> generators.AddRange
                if started then
                    newGenerators
                    |> Seq.map (fun gen -> gen())
                    |> tasks.AddRange
                if noGenerators && waitingQueue.Count > 0 && not nextWaiting then
                    nextWaiting <- true
                    let channel = waitingQueue.Dequeue()
                    whenAnyTaskToChannel channel
                else
                    resetWhenAny()

            | RemoveGenerator (generator, channel) ->

                Log.Logger.LogTrace("TaskSeq.RemoveGenerator nextWaiting:{0}", nextWaiting)
                let index = generators.IndexOf(generator)
                if index > 0 then
                    generators.RemoveAt(index)
                    if started then
                        tasks[index].ContinueWith((fun (t: Task<'T>) ->
                            Log.Logger.LogDebug(t.Exception, "TaskSeq: removed generator task failed")),
                            TaskContinuationOptions.OnlyOnFaulted ||| TaskContinuationOptions.ExecuteSynchronously)
                        |> ignore
                        tasks.RemoveAt(index)
                        resetWhenAny()
                else
                    Log.Logger.LogWarning("TaskSeq: trying to remove non-existing generator")
                channel.SetResult()
        }:> Task).ContinueWith(fun t ->
            if t.IsFaulted then
                let (Flatten ex) = t.Exception
                Log.Logger.LogCritical(ex, "Taskseq mailbox failure")
                stoppedError <- ex
                try
                    try
                        currentMessage |> Option.iter replyFromStoppedLoop
                        while waitingQueue.Count > 0 do
                            waitingQueue.Dequeue().TrySetException(stoppedException ()) |> ignore
                        // wakes a Next already waiting for any task, since no generator will be re-armed
                        resetWhenAnyTcs.TrySetException(stoppedException ()) |> ignore
                    with replyEx ->
                        Log.Logger.LogWarning(replyEx, "TaskSeq failed to fail waiting requests after the loop stopped")
                finally
                    // only after stoppedError is set, so every drained request carries the cause
                    drainStoppedLoop()
            else
                Log.Logger.LogInformation("Taskseq mailbox has stopped normally"))
    |> ignore

    do tasks.Add(resetWhenAnyTcs.Task)
    do generators.Add(Unchecked.defaultof<TaskGenerator<'T>>) // fake generator
    do initialGenerators |> Seq.iter (fun gen -> generators.Add(gen))

    member private this.RestartNext() =
        async {
            let! completedTask = postAndAsyncReply mb WhenAnyTask |> Async.AwaitTask
            Log.Logger.LogTrace("TaskSeq.RestartNext {0}", completedTask.Status)
            if completedTask.IsCanceled then
                return! this.RestartNext()
            else
                return completedTask
        }

    member this.Next() =
        backgroundTask {
            let! completedTask = postAndAsyncReply mb Next |> Async.AwaitTask
            Log.Logger.LogTrace("TaskSeq.Next {0}", completedTask.Status)
            if completedTask.IsCanceled then
                let! restartedTask = this.RestartNext()
                post mb (NextComplete restartedTask)
                return! restartedTask
            else
                post mb (NextComplete completedTask)
                return! completedTask
        }

    member this.AddGenerators generators =
        post mb (AddGenerators generators)

    member this.RemoveGenerator generator =
        postAndAsyncReply mb (fun channel -> RemoveGenerator (generator, channel))
