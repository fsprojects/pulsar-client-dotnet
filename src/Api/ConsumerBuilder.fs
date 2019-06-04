namespace Pulsar.Client.Api

type ConsumerBuilder(client: PulsarClient) =
    let mutable config: ConsumerConfiguration = {
        Topic = ""
        SubscriptionName = ""
    }
    member this.Topic topicName = 
        config <- { config with Topic = topicName }
        this
    member this.SubscriptionName subscriptionName =
        config <- { config with SubscriptionName = subscriptionName }
        this
    member this.SubscribeAsync() =
        client.SubscribeAsync(config)