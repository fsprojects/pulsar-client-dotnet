namespace Pulsar.Client.Api

open System.Threading.Tasks
open Pulsar.Client.Common

type TableViewBuilder<'T> private (createTableViewAsync, config: TableViewConfiguration, schema: ISchema<'T>) =

    let verify(config : TableViewConfiguration) =
        config
        |> (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the reader builder."
                |> fun _ -> c
            )

    internal new(createTableViewAsync, schema) = TableViewBuilder(createTableViewAsync, TableViewConfiguration.Default, schema)
    
    member private this.With(newConfig) =
        TableViewBuilder(createTableViewAsync, newConfig, schema)

    member this.Topic topic =        
        { config with
            Topic = topic
                |> invalidArgIfBlankString "Topic must not be blank."
                |> fun t -> TopicName(t.Trim()) }
        |> this.With

     member this.AutoUpdatePartitions enabled  =
        { config with
            AutoUpdatePartitions = enabled }
        |> this.With

    member this.AutoUpdatePartitionsInterval interval  =
        { config with
            AutoUpdatePartitionsInterval = interval }
        |> this.With

    member this.CreateAsync(): Task<ITableView<'T>> =
        createTableViewAsync(verify config, schema)

    member this.Configuration =
        config
