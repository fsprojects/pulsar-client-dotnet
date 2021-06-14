// Learn more about F# at http://fsharp.org
open System
open Pulsar.Client.Api
open Microsoft.Extensions.Logging
open Simple
open CustomProps
open ReaderApi
open RealWorld
open Schema
open Transaction
open Telemetry
open Oauth2

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
//    runSchema().Wait()
//    runTransaction().Wait()
//    runTelemetry().Wait()
//    runOauth().Wait()

    printfn "Example ended. Press any key to exit"
    Console.ReadKey() |> ignore

    0 // return an integer exit code
