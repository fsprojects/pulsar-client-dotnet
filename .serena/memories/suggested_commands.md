Use PowerShell on Windows. Core commands:
- `dotnet tool restore`
- `dotnet restore`
- `dotnet build -c Release`
- `dotnet run -c Release --project tests/UnitTests/UnitTests.fsproj --no-build`
- `dotnet run -c Release --project tests/IntegrationTests/IntegrationTests.fsproj --no-build --no-spinner --debug`
- `dotnet test tests` (broad test runner from tests README, but CI uses `dotnet run` on Expecto executables)
- `docker compose up -d` from `tests/compose/standalone` to start the local Pulsar cluster used by integration tests
- `docker compose ps` and `docker compose logs init` from `tests/compose/standalone` to inspect cluster startup
Because Main.fs in both test projects forwards CLI args to `Tests.runTestsInAssemblyWithCLIArgs`, Expecto CLI flags can be passed after `--` when using `dotnet run` for focused runs.