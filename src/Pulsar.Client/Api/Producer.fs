namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common

type Producer() =    
    member this.SendAsync (bytes: byte[]) =
        task {
            return { LedgerId = % 0L; EntryId = % 0L; PartitionIndex = 0 }
        }
        // task {
        //    let metadata = new MessageMetadata()
        //    let command = Commands.newSend 
        //}
    member this.AcknowledgeAsync msg =
        Task.FromResult()
