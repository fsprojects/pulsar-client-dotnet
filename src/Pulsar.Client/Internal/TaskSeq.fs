namespace Pulsar.Client.Internal

open System
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open Microsoft.Extensions.Logging

type internal TaskGenerator<'T> = unit -> Task<'T>

type internal TaskSeqMessage<'T> =
    | WhenAnyTask of AsyncReplyChannel<Task<Task<'T>>>
    | Next of AsyncReplyChannel<Task<Task<'T>>>
    | NextComplete of Task<'T>
    | AddGenerators of TaskGenerator<'T> seq
    | RemoveGenerator of TaskGenerator<'T>
    | RestartCompletedTasks

type internal TaskSeq<'T> (initialGenerators: TaskGenerator<'T> seq) =
    let tasks = ResizeArray<Task<'T>>()
    let generators = ResizeArray()
    let mutable started = false
    let mutable nextWaiting = false
    let waitingQueue = Queue()
    let mutable resetWhenAnyTcs = TaskCompletionSource()
    let random = Random()
    
    let whenAnyTask () =
        let tasksCount = tasks.Count
        let randIndex = random.Next(0, tasksCount)
        let mutable i = 0
        let mutable completedFound = false
        let mutable result = null
        while (i < tasksCount && completedFound = false) do
            let index = (randIndex + i) % tasksCount
            let t = tasks.[index]
            if t.IsCompleted then
                completedFound <- true
                result <- t
            else
                i <- i + 1
        if completedFound then
            result |> Task.FromResult
        else
            tasks |> Task.WhenAny
    
    let mb = MailboxProcessor<TaskSeqMessage<'T>>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | WhenAnyTask channel ->
                    
                    Log.Logger.LogTrace("TaskSeq.WhenAnyTask nextWaiting:{0}", nextWaiting)
                    whenAnyTask() |> channel.Reply
                    return! loop ()
                
                | Next channel ->
                    
                    Log.Logger.LogTrace("TaskSeq.Next nextWaiting:{0}", nextWaiting)
                    if not started then
                        for i in [1..generators.Count- 1] do
                            generators.[i]() |> tasks.Add
                        started <- true
                    if nextWaiting || tasks.Count = 1 then
                        waitingQueue.Enqueue channel
                    else
                        nextWaiting <- true
                        whenAnyTask() |> channel.Reply
                    return! loop ()
                        
                | NextComplete completedTask ->
                    
                    Log.Logger.LogTrace("TaskSeq.NextComplete nextWaiting:{0}", nextWaiting)
                    let index = tasks.IndexOf(completedTask)
                    if index > 0 then
                        tasks.[index] <- generators.[index]()
                    else
                        Log.Logger.LogWarning("TaskSeq: generator was removed, but task has completed")
                    if tasks.Count > 1 && waitingQueue.Count > 0 then
                        let channel = waitingQueue.Dequeue()
                        whenAnyTask() |> channel.Reply
                    else
                        nextWaiting <- false
                    return! loop ()
                    
                | RestartCompletedTasks ->
                    
                    Log.Logger.LogTrace("TaskSeq.RestartCompleted nextWaiting:{0}", nextWaiting)
                    for index in 0..tasks.Count-1 do
                        if tasks.[index].IsCompleted then
                            tasks.[index] <- generators.[index]()
                    return! loop()
                    
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
                        whenAnyTask() |> channel.Reply
                    else
                        resetWhenAnyTcs.SetCanceled()
                        resetWhenAnyTcs <- TaskCompletionSource()
                        tasks.[0] <- resetWhenAnyTcs.Task
                    return! loop ()
                    
                | RemoveGenerator generator ->
                    
                    Log.Logger.LogTrace("TaskSeq.RemoveGenerator nextWaiting:{0}", nextWaiting)
                    let index = generators.IndexOf(generator)
                    if index > 0 then
                        generators.RemoveAt(index)
                        if started then
                            tasks.RemoveAt(index)
                    else
                        Log.Logger.LogWarning("TaskSeq: trying to remove non-existing generator")
                    return! loop() 
            }
        loop ()
    )
    
    do tasks.Add(resetWhenAnyTcs.Task)
    do generators.Add(Unchecked.defaultof<TaskGenerator<'T>>) // fake generator
    do initialGenerators |> Seq.iter (fun gen -> generators.Add(gen))
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "Taskseq mailbox failure"))

    member private this.RestartNext() =
        async {
            let! whenAnyTask = mb.PostAndAsyncReply(WhenAnyTask)
            let! completedTask = whenAnyTask |> Async.AwaitTask
            Log.Logger.LogTrace("TaskSeq.RestartNext {0}", completedTask.Status) 
            if completedTask.IsCanceled then
                return! this.RestartNext()
            else
                return completedTask
        }
        
    member this.Next() =
        task {
            let! whenAnyTask = mb.PostAndAsyncReply(Next)
            let! completedTask = whenAnyTask
            Log.Logger.LogTrace("TaskSeq.Next {0}", completedTask.Status) 
            if completedTask.IsCanceled then
                let! restartedTask = this.RestartNext()
                mb.Post(NextComplete restartedTask)
                return! restartedTask
            else
                mb.Post(NextComplete completedTask)
                return! completedTask
        }
        
    member this.AddGenerators generators =
        mb.Post(AddGenerators generators)
        
    member this.RemoveGenerator generator =
        mb.Post(RemoveGenerator generator)
        
    member this.RestartCompletedTasks() =
        mb.Post(RestartCompletedTasks)