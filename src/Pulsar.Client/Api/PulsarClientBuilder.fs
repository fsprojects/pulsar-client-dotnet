namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System

type PulsarClientBuilder private (config: PulsarClientConfiguration) =

    let clientExceptionIfBlankString message arg =
        arg
        |> invalidArgIfBlankString message

    let verify(config : PulsarClientConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.ServiceUrl
                |> clientExceptionIfBlankString "Service Url needs to be specified on the PulsarClientBuilder object.")

    new() = PulsarClientBuilder(PulsarClientConfiguration.Default)

    member this.ServiceUrl (url: string) =

        let useTls = config.UseTls || url.StartsWith("pulsar+ssl")

        PulsarClientBuilder
            { config with
                ServiceUrl = url |> invalidArgIfBlankString "ServiceUrl must not be blank." 
                UseTls = useTls }

    member this.MaxNumberOfRejectedRequestPerConnection num =
        PulsarClientBuilder
            { config with
                MaxNumberOfRejectedRequestPerConnection = num |> invalidArgIfLessThanZero "MaxNumberOfRejectedRequestPerConnection can't be negative" }

    member this.EnableTls useTls =
        PulsarClientBuilder
            { config with
                UseTls = useTls }

    member this.Build() =
        config
        |> verify
        |> PulsarClient
