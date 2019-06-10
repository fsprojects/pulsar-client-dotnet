namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX

type Consumer() =    
    member this.ReceiveAsync() =
        task {
            return { MessageId = { LedgerId = % 0UL; EntryId = % 0UL; PartitionIndex = 0 }; Payload = [||] }
        }
    member this.AcknowledgeAsync msg =
        Task.FromResult()
