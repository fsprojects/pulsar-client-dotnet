namespace Pulsar.Client.Api

open Pulsar.Client.ExceptionHelper

type PulsarClientBuilder private (config: PulsarClientConfiguration) =

    let clientExceptionIfBlankString message arg =
        arg
        |> throwIfBlankString (fun() -> PulsarClientException(message))

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
 
    member __.WithServiceUrl url =
        PulsarClientBuilder
            { config with
                ServiceUrl = url |> invalidArgIfBlankString "ServiceUrl must not be blank." }

    member __.Build() =
        config
        |> verify
        |> PulsarClient
