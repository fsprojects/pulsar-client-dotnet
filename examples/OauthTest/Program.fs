
open OauthTest.runOauth
open Pulsar.Client.Api
open Microsoft.Extensions.Logging
open System


[<EntryPoint>]
let main _ =
    let loggerFactory =
        LoggerFactory.Create(fun builder ->
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole() |> ignore
        )
    PulsarClient.Logger <- loggerFactory.CreateLogger("PulsarLogger")
    
    runOauth().Wait()

    printfn "Example ended. Press any key to exit"
    Console.ReadKey() |> ignore
    0 // return an integer exit code