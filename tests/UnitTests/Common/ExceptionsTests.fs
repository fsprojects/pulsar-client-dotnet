module Pulsar.Client.UnitTests.Common.Exception

open System
open Expecto
open Expecto.Flip
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common
open FSharp.Control.Tasks.V2.ContextInsensitive

[<Tests>]
let tests =

    testList "TopicNameTests" [
        testAsync "Aggregate exception is caught" {
            try
                do!
                    Task.Run(fun () -> raise <| CryptoException "sdf")
                    |> Async.AwaitTask
                    |> Async.Ignore
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
        
        testAsync "Double aggregate exception is caught" {
            try
                do!
                    Task.Run<unit>(fun () -> task {
                        return!
                            Task.Run<unit>(fun () -> task {
                                raise <| CryptoException "sdf"
                            }) |> Async.AwaitTask
                    })
                    |> Async.AwaitTask
                    |> Async.Ignore
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
        
        testAsync "Normal exception is caught" {
            try
                raise <| CryptoException "sdf"
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
    ]

