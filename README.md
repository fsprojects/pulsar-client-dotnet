# pulsar-client-dotnet

## IN EARLY DEVELOPMENT, but you can already try basic pub/sub functionality
### Contributions are most welcome!


Supported pulsar version: **2.4.0**

Features list (from https://github.com/apache/pulsar/wiki/Client-Features-Matrix):

- [X] Basic Producer/Consumer API
- [ ] Partitioned topics
- [ ] Batching
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
    1. Change `pulsarAddress` to point your pulsar cluster
    2. Ensure `public/default` namespace with default settings
    3. Ensure `public/retention` namespace with time or storage size retention configured
 - Send a Pull Request.
