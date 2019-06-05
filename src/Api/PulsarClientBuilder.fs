namespace Pulsar.Client.Api

type PulsarClientBuilder private (config: PulsarClientConfiguration) =
    new() = PulsarClientBuilder(PulsarClientConfiguration.Default)
    
    member this.WithServiceUrl url = 
        PulsarClientBuilder { config with ServiceUrl = url }
    member this.Build() =
        PulsarClient(config)
