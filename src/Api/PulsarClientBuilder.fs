namespace Pulsar.Client.Api

type PulsarClientBuilder() =
    let mutable config: PulsarClientConfiguration = {
        ServiceUrl = ""
    }
    member this.WithServiceUrl url = 
        config <- { config with ServiceUrl = url }
        this
    member this.Build() =
        PulsarClient(config)
