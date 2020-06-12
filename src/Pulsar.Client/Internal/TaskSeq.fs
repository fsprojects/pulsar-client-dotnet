namespace Pulsar.Client.Internal

open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

type TaskGenerator<'T> = unit -> Task<'T>

type internal TaskSeqMessage<'T> =
    | Next of AsyncReplyChannel<Task<'T>>
    | NextComplete of Task<'T>
    | AddGenerator of TaskGenerator<'T>
    | RemoveGenerator of TaskGenerator<'T>

type internal TaskSeq<'T> (initialGenerators: TaskGenerator<'T> seq) as this =
    let tasks = ResizeArray<Task<'T>>()
    let generators = ResizeArray()
    let mutable started = false
    let mutable nextWaiting = false
    let waitingQueue = Queue()
    
    let processWhenAny () =
        let whenAnyTask = tasks |> Task.WhenAny
        task {
            let! completedTask = whenAnyTask
            this.Mb.Post(NextComplete completedTask)
            return! completedTask
        }
    
    let mb = MailboxProcessor<TaskSeqMessage<'T>>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Next channel ->
                    
                    if not started then
                        for generator in generators do
                            generator() |> tasks.Add
                        started <- true
                    if nextWaiting then
                        waitingQueue.Enqueue channel
                    else
                        nextWaiting <- true
                        processWhenAny() |> channel.Reply
                    return! loop ()
                        
                | NextComplete completedTask ->
                    
                    let index = tasks.IndexOf(completedTask)
                    tasks.[index] <- generators.[index]()
                    if waitingQueue.Count > 0 then
                        let channel = waitingQueue.Dequeue()
                        processWhenAny() |> channel.Reply
                    else
                        nextWaiting <- false
                    return! loop ()
                
                | AddGenerator generator ->
                    
                    generator |> generators.Add 
                    if started then
                        generator() |> tasks.Add
                    return! loop ()
                    
                | RemoveGenerator generator ->
                    
                    let index = generators.IndexOf(generator)
                    generators.RemoveAt(index)
                    if started then
                        tasks.RemoveAt(index)
                    return! loop() 
            }
        loop ()
    )
    
    do initialGenerators |> Seq.iter (fun gen -> generators.Add(gen))
    
    member private this.Mb with get(): MailboxProcessor<TaskSeqMessage<'T>> = mb
    
    member this.Next() =
        async {
            let! result = mb.PostAndAsyncReply(Next)
            return! result |> Async.AwaitTask
        }
        
        
    member this.AddGenerator generator =
        mb.Post(AddGenerator generator)
        
    member this.RemoveGenerator generator =
        mb.Post(RemoveGenerator generator)