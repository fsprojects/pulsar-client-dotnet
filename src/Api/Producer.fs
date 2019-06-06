namespace Pulsar.Client.Api

open FSharp.Control.Tasks.ContextSensitive
open System.Threading.Tasks
open FSharp.UMX

type Producer() =    
    member this.SendAsync (bytes: byte[]) =
        task {
            return { LedgerId = % 0L; EntryId = % 0L; PartitionIndex = 0 }
        }
    member this.AcknowledgeAsync msg =
        Task.FromResult()
