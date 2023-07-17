module Main

open Expecto
open Pulsar.Client.Api
open Serilog
open Serilog.Sinks.SystemConsole.Themes
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection

let configureLogging() =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Warning()
            .WriteTo.Console(theme = AnsiConsoleTheme.Code, outputTemplate="[{Timestamp:HH:mm:ss.fff} {Level:u3} {ThreadId}] {Message:lj}{NewLine}{Exception}")
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .CreateLogger()
    let serviceCollection = ServiceCollection()
    let sp =
        serviceCollection
            .AddLogging(fun configure -> configure.AddSerilog(dispose = true) |> ignore)
            .BuildServiceProvider()
    let logger = sp.GetService<ILogger<PulsarClient>>()
    PulsarClient.Logger <- logger

[<EntryPoint>]
let main argv =
    configureLogging()
    let result = Tests.runTestsInAssemblyWithCLIArgs [] argv
    Log.CloseAndFlush() |> ignore
    result
