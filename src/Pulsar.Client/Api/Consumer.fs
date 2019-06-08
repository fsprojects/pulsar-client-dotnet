namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX

type Consumer() =    
    member this.ReceiveAsync() =
        task {
            return { MessageId = { LedgerId = % 0L; EntryId = % 0L; PartitionIndex = 0 } }
        }
    member this.AcknowledgeAsync msg =
        Task.FromResult()
