namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System

type PulsarClientBuilder private (config: PulsarClientConfiguration) =

    let verify(config : PulsarClientConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.ServiceUrl
                |> invalidArgIfDefault "Service Url needs to be specified on the PulsarClientBuilder object.")

    new() = PulsarClientBuilder(PulsarClientConfiguration.Default)

    member this.ServiceUrl (url: string) =

        let uri =
            url
            |> invalidArgIfBlankString "ServiceUrl must not be blank."
            |> Uri
        let useTls = config.UseTls || url.StartsWith("pulsar+ssl")

        PulsarClientBuilder
            { config with
                ServiceUrl = uri
                UseTls = useTls }

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

    member this.Build() =
        config
        |> verify
        |> PulsarClient
