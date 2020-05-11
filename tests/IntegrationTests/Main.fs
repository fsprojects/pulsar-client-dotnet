module Main

open Expecto
open Pulsar.Client.IntegrationTests
open Serilog

[<EntryPoint>]
let main argv =
    Common.configureLogging()
    let result = Tests.runTestsInAssembly defaultConfig argv
    Log.CloseAndFlush() |> ignore
    result
