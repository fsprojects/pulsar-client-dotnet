namespace Pulsar.Client.Api

open Pulsar.Client.Common

type ProducerBuilder private (client: PulsarClient, config: ProducerConfiguration) =

    let producerExceptionIfBlankString message arg =
        arg
        |> throwIfBlankString (fun() -> ProducerException(message))

    let verify(config : ProducerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> producerExceptionIfBlankString "Topic name must be set on the producer builder.")

    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default)

    member __.Topic topic = 
        ProducerBuilder(
            client,
            { config with
                Topic = topic |> invalidArgIfBlankString "Topic must not be blank." })

    member __.ProducerName topic = 
        ProducerBuilder(
            client,
            { config with
                ProducerName = topic |> invalidArgIfBlankString "ProducerName must not be blank." })

    member __.CreateAsync() =
        config
        |> verify
        |> client.CreateProducerAsync