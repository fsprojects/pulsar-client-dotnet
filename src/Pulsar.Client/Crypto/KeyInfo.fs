namespace Pulsar.Client.Crypto

open System.Collections.Generic

type KeyInfo(key: byte [], metadata: Dictionary<string, string>) =

    member this.Key = key

    member this.Metadata = metadata
