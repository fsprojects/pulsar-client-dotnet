# Summary

This PR adds HTTP service URL support to the client and wires lookup operations through a shared lookup abstraction so the client can work with both binary Pulsar endpoints and Pulsar admin/HTTP endpoints.

It also includes follow-up CI fixes required to make the new HTTP lookup path compile and build correctly across the current target frameworks and test configurations.

# Commits Included

- `2fbf421` `feat: Add HttpLookupService to support http serviceUrl (#297)`
- `b0129fa` `Fix CI for HTTP lookup service`

# What Changed

## 1. Added HTTP service URL support

- `ServiceUri.parse` now accepts `http://...` URLs in addition to `pulsar://...`.
- `PulsarClientConfiguration` now carries the parsed scheme.
- `PulsarClientBuilder.ServiceUrl(...)` stores both the resolved addresses and the parsed scheme.
- `PulsarClient` now selects the lookup implementation based on the configured scheme:
  - `BinaryLookupService` for `pulsar://...`
  - `HttpLookupService` for `http://...`

## 2. Introduced a shared lookup abstraction

- Added [`src/Pulsar.Client/Internal/ILookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ILookupService.fs) to unify the lookup capabilities the client needs:
  - partition metadata lookup
  - broker lookup
  - namespace topic discovery
  - schema lookup
- Updated the client and internal components to depend on `ILookupService` instead of directly depending on the binary lookup implementation.

Affected areas include:

- [`src/Pulsar.Client/Api/PulsarClient.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Api/PulsarClient.fs)
- [`src/Pulsar.Client/Internal/ConnectionHandler.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ConnectionHandler.fs)
- [`src/Pulsar.Client/Internal/ConsumerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ConsumerImpl.fs)
- [`src/Pulsar.Client/Internal/MultiTopicsConsumerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/MultiTopicsConsumerImpl.fs)
- [`src/Pulsar.Client/Internal/MultiTopicsReaderImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/MultiTopicsReaderImpl.fs)
- [`src/Pulsar.Client/Internal/PartitionedProducerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/PartitionedProducerImpl.fs)
- [`src/Pulsar.Client/Internal/ProducerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ProducerImpl.fs)
- [`src/Pulsar.Client/Internal/ReaderImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ReaderImpl.fs)
- [`src/Pulsar.Client/Internal/TransactionMetaStoreHandler.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/TransactionMetaStoreHandler.fs)
- [`src/Pulsar.Client/Transaction/TransactionCoordinatorClient.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Transaction/TransactionCoordinatorClient.fs)

## 3. Added `HttpLookupService`

- Added [`src/Pulsar.Client/Internal/HttpLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/HttpLookupService.fs).
- The new implementation uses Pulsar HTTP/admin endpoints to support the same lookup operations exposed by `ILookupService`.
- Implemented support for:
  - partitioned topic metadata lookup
  - broker lookup
  - namespace topic enumeration
  - schema lookup, including key/value schema handling
- Added the file to [`src/Pulsar.Client/Pulsar.Client.fsproj`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Pulsar.Client.fsproj).

## 4. Updated `BinaryLookupService`

- [`src/Pulsar.Client/Internal/BinaryLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/BinaryLookupService.fs) now implements the shared `ILookupService` contract.
- During conflict resolution, duplicate class members were removed so the file exposes one consistent lookup surface through the interface-backed implementation.

## 5. Added tests for HTTP lookup behavior

- Added new integration coverage in [`tests/IntegrationTests/HttpLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/HttpLookupService.fs).
- Covered scenarios include:
  - basic produce/consume flow using an HTTP service URL
  - concurrent send/receive flow
  - topic pattern discovery via namespace lookup
  - schema lookup and key/value schema round-trip behavior
- Registered the new test file in [`tests/IntegrationTests/IntegrationTests.fsproj`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/IntegrationTests.fsproj).

## 6. Added unit coverage for HTTP service URL parsing

- Extended [`tests/UnitTests/Api/ServiceUriTests.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/UnitTests/Api/ServiceUriTests.fs) to verify:
  - HTTP scheme parsing
  - HTTP scheme preservation in the parsed result
  - default HTTP port behavior

## 7. Documentation update

- Updated [`README.md`](/Users/rbt/code/pulsar-client-dotnet/README.md) to reflect the new HTTP service URL capability.

# CI Fixes Included In This Branch

The original cherry-picked feature introduced two CI breakages. This branch fixes both:

## 1. netstandard2.0 compatibility in `HttpLookupService`

The initial implementation used newer APIs that are not available in the project target framework:

- `SocketsHttpHandler`
- `HttpRequestException.StatusCode`

The implementation was rewritten to use APIs that work with the current project targets:

- replaced `SocketsHttpHandler` with `HttpClientHandler`
- switched schema lookup handling to explicit `HttpResponseMessage.StatusCode` checks
- kept resource ownership explicit by disposing HTTP responses and streams in the same control flow that creates them

## 2. `NOTLS` integration test build fix

[`tests/IntegrationTests/Common.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/Common.fs) originally declared `pulsarHttpAddress` inside a `#if !NOTLS` block, but it is used by code that still compiles under `NOTLS`.

This branch moves `pulsarHttpAddress` outside that conditional block so both of these builds succeed:

- default integration test build
- `-p:DefineConstants=NOTLS` integration test build

# Files Changed

- [`README.md`](/Users/rbt/code/pulsar-client-dotnet/README.md)
- [`src/Pulsar.Client/Api/Configuration.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Api/Configuration.fs)
- [`src/Pulsar.Client/Api/PulsarClient.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Api/PulsarClient.fs)
- [`src/Pulsar.Client/Api/PulsarClientBuilder.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Api/PulsarClientBuilder.fs)
- [`src/Pulsar.Client/Common/ServiceUri.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Common/ServiceUri.fs)
- [`src/Pulsar.Client/Internal/BinaryLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/BinaryLookupService.fs)
- [`src/Pulsar.Client/Internal/ConnectionHandler.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ConnectionHandler.fs)
- [`src/Pulsar.Client/Internal/ConsumerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ConsumerImpl.fs)
- [`src/Pulsar.Client/Internal/HttpLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/HttpLookupService.fs)
- [`src/Pulsar.Client/Internal/ILookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ILookupService.fs)
- [`src/Pulsar.Client/Internal/MultiTopicsConsumerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/MultiTopicsConsumerImpl.fs)
- [`src/Pulsar.Client/Internal/MultiTopicsReaderImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/MultiTopicsReaderImpl.fs)
- [`src/Pulsar.Client/Internal/PartitionedProducerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/PartitionedProducerImpl.fs)
- [`src/Pulsar.Client/Internal/ProducerImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ProducerImpl.fs)
- [`src/Pulsar.Client/Internal/ReaderImpl.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/ReaderImpl.fs)
- [`src/Pulsar.Client/Internal/TransactionMetaStoreHandler.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Internal/TransactionMetaStoreHandler.fs)
- [`src/Pulsar.Client/Pulsar.Client.fsproj`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Pulsar.Client.fsproj)
- [`src/Pulsar.Client/Transaction/TransactionCoordinatorClient.fs`](/Users/rbt/code/pulsar-client-dotnet/src/Pulsar.Client/Transaction/TransactionCoordinatorClient.fs)
- [`tests/IntegrationTests/Common.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/Common.fs)
- [`tests/IntegrationTests/HttpLookupService.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/HttpLookupService.fs)
- [`tests/IntegrationTests/IntegrationTests.fsproj`](/Users/rbt/code/pulsar-client-dotnet/tests/IntegrationTests/IntegrationTests.fsproj)
- [`tests/UnitTests/Api/ServiceUriTests.fs`](/Users/rbt/code/pulsar-client-dotnet/tests/UnitTests/Api/ServiceUriTests.fs)

# Validation

The following builds were run successfully on this branch:

```bash
dotnet build src/Pulsar.Client/Pulsar.Client.fsproj -c Release
dotnet build tests/UnitTests/UnitTests.fsproj -c Release
dotnet build tests/IntegrationTests/IntegrationTests.fsproj -c Release --no-restore
dotnet build tests/IntegrationTests/IntegrationTests.fsproj -c Release -p:DefineConstants=NOTLS --no-restore
```

Note: running unit tests directly on the local macOS environment still depends on native `snappy` and `zstd` libraries being installed. The CI workflows already install those dependencies on Linux.
