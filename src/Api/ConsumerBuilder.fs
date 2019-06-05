namespace Pulsar.Client.Api

type ConsumerBuilder private (client: PulsarClient, config: ConsumerConfiguration) =
    new(client: PulsarClient) = ConsumerBuilder(client, ConsumerConfiguration.Default)

    member this.Topic topicName = 
        ConsumerBuilder(client, { config with Topic = topicName })
        
    member this.SubscriptionName subscriptionName =
        ConsumerBuilder(client, { config with SubscriptionName = subscriptionName })

    member this.SubscribeAsync() =
        client.SubscribeAsync(config)