namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open System.Net

type internal ServiceNameResolver (config: PulsarClientConfiguration) =
    member this.ResolveHost() =
        IPEndPoint(IPAddress.Parse(config.ServiceUrl), 6650)
    