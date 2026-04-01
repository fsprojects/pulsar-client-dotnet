# pulsar-client-dotnet
![Pulsar.Client](https://github.com/fsprojects/pulsar-client-dotnet/raw/develop/images/pulsar-client-dotnet.png)

[![.NET Core Windows](https://github.com/fsprojects/pulsar-client-dotnet/workflows/.NET%20Core%20Windows/badge.svg)](https://github.com/fsprojects/pulsar-client-dotnet/actions?query=workflow%3A%22.NET+Core+Windows%22)
[![.NET Core Ubuntu](https://github.com/fsprojects/pulsar-client-dotnet/workflows/.NET%20Core%20Ubuntu/badge.svg)](https://github.com/fsprojects/pulsar-client-dotnet/actions?query=workflow%3A%22.NET+Core+Ubuntu%22)

### Contributions and stars ★ are most welcome!

[Pulsar.Client nuget](https://www.nuget.org/packages/Pulsar.Client) |
[Pulsar.Client.Otel nuget](https://www.nuget.org/packages/Pulsar.Client.Otel/)

Supported pulsar cluster versions: **2.4+**

Find examples of usage in [/examples](https://github.com/fsharplang-ru/pulsar-client-dotnet/tree/develop/examples) folder. We are trying to keep api similar to the Java client, so please take a look at [it's documentation](https://pulsar.apache.org/docs/en/client-libraries-java/#connection-urls) to understand possible options available. You can also join telegram chat https://t.me/pro_pulsar for further discussions.

Features list (based on [Client Feature Matrix](https://github.com/apache/pulsar/wiki/PIP-108%3A-Pulsar-Feature-Matrix-%28Client-and-Function%29)):

- [X] Basic Producer/Consumer API
- [X] Partitioned topics
- [X] Batching
- [X] Chunking
- [X] Compression
- [X] TLS
- [X] Authentication (token, tls, oauth2)
- [X] Reader API
- [X] Proxy Support
- [X] Effectively-Once
- [X] Schema (All types, Multi-version)
- [X] Consumer seek
- [X] Multi-topics consumer
- [X] Topics regex consumer
- [X] Compacted topics
- [X] User defined properties
- [X] Reader hasMessageAvailable
- [X] Hostname verification
- [X] Multi Hosts Service Url
- [X] Key_shared subscription
- [X] Key based batcher
- [X] Negative Acknowledge
- [X] Delayed/scheduled messages	
- [X] Dead Letter Policy
- [X] Interceptors
- [X] Transactions
- [X] Statistics
- [X] End-to-end Encryption
- [X] SubscriptionInitialPosition
- [X] Cumulative Ack
- [X] Batch-Index Ack
- [ ] SNI Routing
- [X] Table view

## Quick contributing guide

#### Common steps before building

 - Fork and clone locally
 - Install dotnet tools: `dotnet tool restore`
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
        * `brew install streamlio/homebrew-formulae/pulsar`
        * `brew install streamlio/homebrew-formulae/bookkeeper`
        * `brew services start pulsar`
        * `brew services start bookkeeper`
    1. Run commands in `/tests/IntegrationTests/commands.txt`
    1. Change `pulsarAddress` in Common.fs to point your pulsar cluster
    1. Ensure `public/default` namespace with default settings
    1. Ensure `public/retention` namespace with time or storage size retention configured
 - Send a Pull Request

#### Maintaners

  * @Lanayx
