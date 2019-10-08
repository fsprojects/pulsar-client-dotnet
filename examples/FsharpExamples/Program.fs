// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open Microsoft.Extensions.Logging
open Simple
open CustomProps

[<EntryPoint>]
let main argv =

    let loggerFactory =
        LoggerFactory.Create(fun builder ->
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole() |> ignore
        )
    PulsarClient.Logger <- loggerFactory.CreateLogger("PulsarLogger")

    runSimple()
    |> Async.AwaitTask
    |> Async.RunSynchronously

    runCustomProps()
    |> Async.AwaitTask
    |> Async.RunSynchronously

    printfn "Example ended. Press any key to exit"
    Console.ReadKey() |> ignore

    0 // return an integer exit code
