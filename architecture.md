![image](https://github.com/fsprojects/pulsar-client-dotnet/assets/3329606/a7dc577e-beed-4208-818e-fe799f058637)

Architecturally pulsar-client-dotnet is a series of actors that send messages to each other. Each of them uses [Unbounded Channel](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/) as a message queue and processes messages one by one in loop.
Actors communicate with each other by sending messages between each other. Messages are modeled as [discriminated unions](https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/discriminated-unions)

Connections to broker are reused (one connection per server) between producers and consumers. 

PulsarClient class serves as un umbrella class and stores reference to all other entities, i.e. consumers, producers, connection pool, lookup service and schema providers
