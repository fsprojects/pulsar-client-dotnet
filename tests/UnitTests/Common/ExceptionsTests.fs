module Pulsar.Client.UnitTests.Common.Exception

open System
open Expecto
open Expecto.Flip
open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common


[<Tests>]
let tests =

    testList "TopicNameTests" [
        testTask "Aggregate exception is caught" {
            try
                do!
                    Task.Run(fun () -> raise <| CryptoException "sdf")
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
        
        testTask "Double aggregate exception is caught" {
            try
                do!
                    Task.Run<unit>(fun () -> task {
                        return!
                            Task.Run<unit>(fun () -> task {
                                raise <| CryptoException "sdf"
                            }) 
                    })
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
        
        testTask "Normal exception is caught" {
            try
                raise <| CryptoException "sdf"
            with Flatten ex ->
                Expect.isTrue "" (ex :? CryptoException)
        }
    ]

