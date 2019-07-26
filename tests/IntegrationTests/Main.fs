module Main

open Expecto

[<EntryPoint>]
let main argv =
    Tests.runTestsInAssembly { defaultConfig with ``parallel`` = false } argv
