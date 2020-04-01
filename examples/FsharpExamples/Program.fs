// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open Microsoft.Extensions.Logging
open Simple
open CustomProps
open ReaderApi
open RealWorld

[<EntryPoint>]
let main _ =

    let loggerFactory =
        LoggerFactory.Create(fun builder ->
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole() |> ignore
        )
    PulsarClient.Logger <- loggerFactory.CreateLogger("PulsarLogger") 

    runSimple().Wait()
//    runTlsAuthentication().Wait()
//    runCustomProps().Wait()
//    runReader().Wait()
//    runRealWorld(PulsarClient.Logger).Wait()

    printfn "Example ended. Press any key to exit"
    Console.ReadKey() |> ignore

    0 // return an integer exit code
