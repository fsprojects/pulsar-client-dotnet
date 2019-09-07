# pulsar-client-dotnet

### Contributions are most welcome!

[Pulsar.Client nuget](https://www.nuget.org/packages/Pulsar.Client)

Supported pulsar version: **2.4.0**

Find examples of usage in [/examples](https://github.com/fsharplang-ru/pulsar-client-dotnet/tree/develop/examples) folder. We are trying to keep api similar to the Java client, so please take a look at [it's documentation](https://pulsar.apache.org/docs/en/client-libraries-java/#client-configuration) to understand possible options available.

Features list (from https://github.com/apache/pulsar/wiki/Client-Features-Matrix):

- [X] Basic Producer/Consumer API
- [ ] Partitioned topics
- [X] Batching
- [ ] Compression
- [ ] TLS
- [ ] Authentication
- [ ] Reader API
- [X] Proxy Support
- [ ] Effectively-Once
- [ ] Schema
- [ ] Consumer seek
- [ ] Multi-topics consumer
- [ ] Topics regex consumer
- [ ] Compacted topics
- [ ] User defined properties producer/consumer
- [ ] Reader hasMessageAvailable
- [ ] Hostname verification
- [ ] Multi Hosts Service Url support


## Quick contributing guide

 - Fork and clone locally.
 - Build the solution with `build.cmd` or `build.sh`. (dotnet core sdk required) This will install required tools and then you can use any IDE to build solution
 - Make youre modifications
 - Run Unit tests
 - If changes are made in Client logic, run Integration tests. Before runnint do the following.
    1. Change `pulsarAddress` in Common.fs to point your pulsar cluster
    2. Ensure `public/default` namespace with default settings
    3. Ensure `public/retention` namespace with time or storage size retention configured
 - Send a Pull Request.
