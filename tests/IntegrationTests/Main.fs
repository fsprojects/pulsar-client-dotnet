module Main

open Expecto
open Pulsar.Client.IntegrationTests

[<EntryPoint>]
let main argv =
    Common.configureLogging()
    Tests.runTestsInAssembly defaultConfig argv
