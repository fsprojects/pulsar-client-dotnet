namespace Pulsar.Client.Internal

open System.Collections.Concurrent
open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Common
open Pulsar.Client.Api

open System

type internal TableViewImpl<'T> private (reader: IReader<'T>) =
    let data = ConcurrentDictionary<String, 'T>()
    
    member private this.HandleMessage(msg: Message<'T>) = 
        if not (String.IsNullOrEmpty(%msg.Key)) then
            data.TryAdd(%msg.Key, msg.GetValue()) |> ignore

    member private this.ReadTailMessages(reader: IReader<'T>) = 
        task {
            let! msg = reader.ReadNextAsync()
            this.HandleMessage(msg)
            this.ReadTailMessages(reader) |> ignore
        }

    member private this.ReadAllExistingMessages(reader: IReader<'T>) = 
        task {
            let! hasMessage = reader.HasMessageAvailableAsync()
            if hasMessage then
                let! msg = reader.ReadNextAsync()
                this.HandleMessage(msg)
                return! this.ReadAllExistingMessages(reader)
            else
                this.ReadTailMessages(reader) |> ignore
                return 0;
        }

    static member internal Init(createReader: _ -> Task<IReader<'T>>) =
        task {
            let! reader = createReader()
            let tableView = TableViewImpl(reader)
            tableView.ReadAllExistingMessages(reader).Wait()
            return tableView
        }

    interface ITableView<'T> with
        member this.Count
            with get () = data.Count

         member this.Keys
            with get () = data.Keys

        member this.Values
            with get () = data.Values

        member this.Item
           with get(key) = data[key]

        member this.GetEnumerator() =
            data.GetEnumerator()

        member this.GetEnumerator(): Collections.IEnumerator =
            data.GetEnumerator()

        member this.ContainsKey(key) =
            data.ContainsKey(key)

        member this.TryGetValue(key, value) =
            data.TryGetValue(key, &value)
    
    interface IAsyncDisposable with
       member this.DisposeAsync() =
           task {
               reader.DisposeAsync() |> ignore
           } |> ValueTask
