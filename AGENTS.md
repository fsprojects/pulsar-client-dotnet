# Repository Notes

## Toolchain
- `global.json` pins the SDK to `.NET 10`; use a .NET 10 SDK even though the main library projects target `net8.0`.
- Restore order used by the repo and CI: `dotnet tool restore`, `dotnet restore`, then `dotnet build -c Release`.
- There is no checked-in formatter or lint config. Do not assume Fantomas or a lint step exists.

## Layout
- Main package: `src/Pulsar.Client`.
- Support packages: `src/Pulsar.Client.Proto` (C# proto support) and `src/Pulsar.Client.Otel` (OpenTelemetry plugin).
- Tests: `tests/UnitTests` and `tests/IntegrationTests`.
- Local Pulsar test environment: `tests/compose/standalone`.

## Entry Points
- Public client construction starts at `src/Pulsar.Client/Api/PulsarClientBuilder.fs`.
- Runtime orchestration is centered in `src/Pulsar.Client/Api/PulsarClient.fs`.
- Internals are actor-like and communicate through `System.Threading.Channels`; most broker-facing flow lives under `src/Pulsar.Client/Internal`.

## Testing
- Tests use Expecto, not xUnit/NUnit.
- CI builds once, then runs the test executables with `dotnet run --no-build` rather than relying on `dotnet test`.
- Fast verification after library changes: `dotnet run -c Release --project tests/UnitTests/UnitTests.fsproj --no-build`
- Both test projects pass CLI args into `Tests.runTestsInAssemblyWithCLIArgs`, so you can use Expecto filters by appending args after `--` to `dotnet run`.

## Integration Tests
- Don't run integration test until explicitly asked
- Start the local broker from `tests/compose/standalone` with `docker compose up -d`.
- The checked-in integration defaults in `tests/IntegrationTests/Common.fs` expect local endpoints `pulsar://127.0.0.1:6650`, `http://127.0.0.1:8080`, and TLS on `6651`; the provided compose file matches these.
- CI integration command: `dotnet run -c Release --project tests/IntegrationTests/IntegrationTests.fsproj --no-build --no-spinner --debug`
- TLS integration tests can be disabled locally with the `NOTLS` compile constant as described in `tests/README.md`.

## Platform Notes
- Ubuntu/macOS integration-related compression support requires native `snappy` and `zstd` packages; see `README.md` and `.github/workflows/dotnetcore-ubuntu.yml`.
