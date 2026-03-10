namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common


type PulsarClientBuilder private (config: PulsarClientConfiguration) =

    [<Literal>]
    let MIN_STATS_INTERVAL_SECONDS = 1

    let verify(config : PulsarClientConfiguration) =
        config
        |> invalidArgIf (fun c ->
                c.ServiceAddresses.Length = 0 && c.ServiceUrlProvider.IsNone
            ) "ServiceUrl or ServiceUrlProvider needs to be specified on the PulsarClientBuilder object."
        |> invalidArgIf (fun c ->
                c.ServiceAddresses.Length > 0 && c.ServiceUrlProvider.IsSome
            ) "Can only chose one way ServiceUrl or ServiceUrlProvider."
        |> (fun c ->
                c.ServiceUrlProvider
                |> Option.map _.GetServiceUrl()
                |> Option.map (invalidArgIfBlankString "Cannot get service url from service url provider.")
                |> Option.map (fun url ->
                    match ServiceUri.parse url with
                    | Result.Ok serviceUri ->
                        { config with ServiceAddresses = serviceUri.Addresses; UseTls = serviceUri.UseTls; Scheme = serviceUri.Scheme }
                    | Result.Error message -> invalidArg null message
                    )
                |> Option.defaultValue c
            )


    new() = PulsarClientBuilder(PulsarClientConfiguration.Default)

    member this.ServiceUrl (url: string) =
        match url |> ServiceUri.parse with
        | Result.Ok serviceUri ->
            PulsarClientBuilder { config with ServiceAddresses = serviceUri.Addresses; UseTls = serviceUri.UseTls ; Scheme = serviceUri.Scheme }
        | Result.Error message -> invalidArg null message

    member this.ServiceUrlProvider (provider: IServiceUrlProvider) =
        PulsarClientBuilder
            { config with ServiceUrlProvider = Some provider }

    member this.OperationTimeout operationTimeout =
        PulsarClientBuilder
            { config with
                OperationTimeout = operationTimeout }

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
            
    member this.TlsCertificate tlsCertificate =
        PulsarClientBuilder
            { config with
                TlsCertificate = tlsCertificate }

    member this.Authentication authentication =
        PulsarClientBuilder
            { config with
                Authentication = authentication |> invalidArgIfDefault "Authentication can't be null" }

    member this.TlsProtocols protocol =
        PulsarClientBuilder
            { config with
                TlsProtocols = protocol }

    member this.StatsInterval interval =
        PulsarClientBuilder
            { config with
                StatsInterval = interval |> invalidArgIf (fun arg ->
                arg <> TimeSpan.Zero && arg < TimeSpan.FromSeconds(float MIN_STATS_INTERVAL_SECONDS)) (sprintf "Stats interval should be greater than %i s" MIN_STATS_INTERVAL_SECONDS) }

    member this.ListenerName name =
        PulsarClientBuilder
            { config with
                ListenerName =
                    name
                    |> invalidArgIfBlankString "Param listenerName must not be blank."
                    |> _.Trim() }

    member this.MaxLookupRedirects maxLookupRedirects =
        PulsarClientBuilder
            { config with
                MaxLookupRedirects = maxLookupRedirects }

    member this.EnableTransaction enableTransaction =
        PulsarClientBuilder
            { config with
                EnableTransaction = enableTransaction }

    member this.KeepAliveInterval keepAliveInterval =
        PulsarClientBuilder
            { config with
                KeepAliveInterval = keepAliveInterval }

    member this.BuildAsync() =
        let client =
            config
            |> verify
            |> PulsarClient
        backgroundTask {
            do! client.Init()
            return client
        }

    member this.Configuration =
        config
