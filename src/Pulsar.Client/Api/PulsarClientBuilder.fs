namespace Pulsar.Client.Api

open Pulsar.Client.Common

type PulsarClientBuilder private (config: PulsarClientConfiguration) =

    let verify(config : PulsarClientConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.ServiceAddresses
                |> invalidArgIf (fun addresses -> addresses |> List.isEmpty) "Service Url needs to be specified on the PulsarClientBuilder object.")

    new() = PulsarClientBuilder(PulsarClientConfiguration.Default)

    member this.ServiceUrl (url: string) =
        match url |> ServiceUri.parse with
        | (Result.Ok serviceUri) ->
            PulsarClientBuilder { config with ServiceAddresses = serviceUri.Addresses; UseTls = serviceUri.UseTls }
        | (Result.Error message) -> invalidArg null message

    member this.MaxNumberOfRejectedRequestPerConnection num =
        PulsarClientBuilder
            { config with
                MaxNumberOfRejectedRequestPerConnection = num |> invalidArgIfLessThanZero "MaxNumberOfRejectedRequestPerConnection can't be negative" }

    member this.EnableTls useTls =
        PulsarClientBuilder
            { config with
                UseTls = useTls }

    member this.EnableTlsHostnameVerification enableTlsHostnameVerification =
        PulsarClientBuilder
            { config with
                TlsHostnameVerificationEnable = enableTlsHostnameVerification }

    member this.AllowTlsInsecureConnection allowTlsInsecureConnection =
        PulsarClientBuilder
            { config with
                TlsAllowInsecureConnection = allowTlsInsecureConnection }

    member this.TlsTrustCertificate tlsTrustCertificate =
        PulsarClientBuilder
            { config with
                TlsTrustCertificate = tlsTrustCertificate }

    member this.Authentication authentication =
        PulsarClientBuilder
            { config with
                Authentication = authentication |> invalidArgIfDefault "Authentication can't be null" }

    member this.TlsProtocols protocol =
        PulsarClientBuilder
            { config with
                TlsProtocols = protocol }

    member this.Build() =
        config
        |> verify
        |> PulsarClient
