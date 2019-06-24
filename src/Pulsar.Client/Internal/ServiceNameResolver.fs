namespace Pulsar.Client.Internal

open System
open System.Net
open Pulsar.Client.Api

//TODO: implement and move into separate file
type internal ServiceUri = Uri

type internal ServiceNameResolver(config: PulsarClientConfiguration) =

    member __.GetServiceUrl() = config.ServiceUrl

    member __.UpdateServiceUrl (serviceUrl: string) =
        let config = { config with ServiceUrl = serviceUrl }
        ServiceNameResolver(config)

    member __.GetServiceUri() = ServiceUri(config.ServiceUrl)

    member this.ResolveHost() =
        let uri = this.GetServiceUri()
        DnsEndPoint(uri.Host, uri.Port)

    member __.ResolveHostUri() = Uri(config.ServiceUrl)
