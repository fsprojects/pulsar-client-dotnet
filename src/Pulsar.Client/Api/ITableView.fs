namespace Pulsar.Client.Api

open System
open System.Collections.Generic

type ITableView<'T> =
    inherit IAsyncDisposable
    inherit IReadOnlyDictionary<String, 'T>
