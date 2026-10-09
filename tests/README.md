# Testing Pulsar .NET Client

## Run All Tests (TL;DR)
```bash
cd pulsar-client-dotnet
dotnet test tests
```

## Unit Tests
You can run the unit tests without installing or running Pulsar on your machine.

### Within your IDE
You can run the unit tests within your IDE. For example, in Visual Studio, you can right-click on the `UnitTests` project and select `Run Tests`.
On JetBrains Rider, you can right-click on the `UnitTests` project and select `Run UnitTests`.

### From the command line
You can run the unit tests from the command line. From the root of the repository, run the following command:
```powershell
dotnet build tests\UnitTests\UnitTests.fsproj -c Release
dotnet run -c Release --project tests\UnitTests\UnitTests.fsproj --no-build
```

Producer disposal and queued-payload lifetime regression tests use a concrete connection backed by in-memory pipes, so they do not require a broker.
To run only these tests, append `-- --filter-test-list ProducerImpl` to the `dotnet run` command.

TaskSeq regression tests cover generator removal during pending reads and resuming queued reads after all generators have been replaced.
Select them with `-- --filter-test-list TaskSeq`.

## Integration Tests

### Prerequisites
You must have a Pulsar cluster running to run the integration tests.

#### Disable the TLS tests (optional)
It is recommended to disable the TLS tests if you don't have a Pulsar cluster running with TLS enabled.

In Rider, You can disable the TLS tests by right clicking on the `IntegrationTests` project and selecting `Properties`.
Then, in the `Debug` tab, add `NOTLS` to the `Define constants` field.

Alternatively, you can simply add the following lines in `IntegrationTests.fsproj`:
```xml
<PropertyGroup>
    <DefineConstants>TRACE;NOTLS</DefineConstants>
</PropertyGroup>
```

#### Using the provided Docker Compose file
You can run a Pulsar cluster locally using Docker.
You can use the provided `docker-compose.yml` file to run a Pulsar cluster locally.
From the root of the repository, run the following command:
```bash
cd pulsar-client-dotnet/tests/IntegrationTests/compose
docker-compose up -d
```
Since the docker-compose will expose the ports at the default port number, you shouldn't have to update the `pulsarAddress` in `Common.fs` to point to your Pulsar cluster.

#### Using your own Pulsar cluster (with minikube)
You can run a Pulsar cluster locally using minikube.
Make sure that you update the `pulsarAddress` in `Common.fs` to point to your Pulsar cluster.

### Running the tests
Mailbox failure regressions cover broker close replies, pending receives, and multi-topic poller shutdown.
Run this subset against the local broker from the repository root:
```powershell
dotnet build tests\IntegrationTests\IntegrationTests.fsproj -c Release
Push-Location tests\IntegrationTests
dotnet run -c Release --no-build -- --filter-test-case mailbox --no-spinner
Pop-Location
```

The `Multitopic.Pattern topic removal keeps remaining consumers active` test covers intentional child disposal during pattern refresh.
Select it with `--filter-test-case "Pattern topic removal"` instead of the mailbox filter above.

Multi-topic seek regressions stagger child seeks, check every message after the reset, and verify recovery after a rejected resolver.
They also cover buffered reads and explicit redelivery with Shared and Exclusive subscriptions. Select them with `--filter-test-case "Multi-topic seek"`.

You can run the integration tests from the command line. From the root of the repository, run the following command:
```bash
cd pulsar-client-dotnet/tests/IntegrationTests
dotnet test tests/IntegrationTests.csproj
```
