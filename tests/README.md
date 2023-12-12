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
```bash
cd pulsar-client-dotnet/tests/UnitTests
dotnet test tests/UnitTests.csproj
```

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
You can run the integration tests from the command line. From the root of the repository, run the following command:
```bash
cd pulsar-client-dotnet/tests/IntegrationTests
dotnet test tests/IntegrationTests.csproj
```
