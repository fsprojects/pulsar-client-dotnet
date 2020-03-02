# pulsar-client-dotnet

![.NET Core Windows](https://github.com/fsharplang-ru/pulsar-client-dotnet/workflows/.NET%20Core%20Windows/badge.svg)
![.NET Core Ubuntu](https://github.com/fsharplang-ru/pulsar-client-dotnet/workflows/.NET%20Core%20Ubuntu/badge.svg)
### Contributions are most welcome!

[Pulsar.Client nuget](https://www.nuget.org/packages/Pulsar.Client)

Supported pulsar cluster versions: **2.4+**

Find examples of usage in [/examples](https://github.com/fsharplang-ru/pulsar-client-dotnet/tree/develop/examples) folder. We are trying to keep api similar to the Java client, so please take a look at [it's documentation](https://pulsar.apache.org/docs/en/client-libraries-java/#client-configuration) to understand possible options available. You can also join telegram chat https://t.me/pro_pulsar for further discussions.

Features list (from https://github.com/apache/pulsar/wiki/Client-Features-Matrix):

- [X] Basic Producer/Consumer API
- [X] Partitioned topics
- [X] Batching
- [X] Compression
- [X] TLS
- [X] Authentication (token-based)
- [X] Reader API
- [X] Proxy Support
- [ ] Effectively-Once
- [ ] Schema
- [X] Consumer seek
- [ ] Multi-topics consumer
- [ ] Topics regex consumer
- [X] Compacted topics
- [X] User defined properties producer/consumer
- [X] Reader hasMessageAvailable
- [X] Hostname verification
- [X] Multi Hosts Service Url support
- [X] Key_shared
- [X] key based batcher
- [X] Negative Acknowledge
- [X] Delayed Delivery Messages
- [X] Dead Letter Policy
- [ ] Interceptors


## Quick contributing guide

#### Common steps before building

 - Fork and clone locally
 - Install Paket dotnet tool:
   * Globally `dotnet tool install paket -g`
   * Locally `dotnet tool install paket --tool-path .paket`
 - Install packages: `paket install` or `.paket/paket install` (if installed locally)
 - Restore packages: `dotnet restore`
 
#### MacOS steps before building:

 - Install Snappy: `brew install snappy`
 - Install Libzstd: `brew install zstd`
 
#### Ubuntu steps before building:
 
 - Install Snappy: `sudo apt-get install -y libsnappy-dev`
 - Install Libzstd: `sudo apt-get install -y libzstd-dev`
 
#### Building and Testing

 - Build the solution: `dotnet build` (dotnet core sdk required) This will install required tools and then you can use any IDE to build solution
 - Make your modifications
 - Run unit tests: `cd tests/UnitTests` && `dotnet test` 
 - (Optional) If changes are made in Client logic, run Integration tests. Before running do the following
    1. Install pulsar cluster:
        * MacOS guide:
        * `brew tap streamlio/homebrew-formulae`
        * `brew install streamlio/homebrew-formulae/heron`
        * `brew install streamlio/homebrew-formulae/pulsar`
        * `brew install streamlio/homebrew-formulae/bookkeeper`
        * `brew services start heron`
        * `brew services start pulsar`
        * `brew services start bookkeeper`
    1. Run commands in `/tests/IntegrationTests/commands.txt`
    1. Change `pulsarAddress` in Common.fs to point your pulsar cluster
    1. Ensure `public/default` namespace with default settings
    1. Ensure `public/retention` namespace with time or storage size retention configured
 - Send a Pull Request
