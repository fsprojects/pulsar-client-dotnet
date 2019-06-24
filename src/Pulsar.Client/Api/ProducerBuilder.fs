namespace Pulsar.Client.Api

type ProducerBuilder private (client: PulsarClient, config: ProducerConfiguration) =
    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default)

    member this.Topic topicName = 
        ProducerBuilder(client, { config with Topic = topicName })

    member this.CreateAsync() =
        client.CreateProducerAsync(config)