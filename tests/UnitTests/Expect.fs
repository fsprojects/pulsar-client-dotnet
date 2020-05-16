namespace Pulsar.Client.UnitTests

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Common

module internal Expect =

    let throwsT2<'texn when 'texn :> exn> f =
        try
            f ()
            failtestf "Expected f to throw."
        with Flatten e ->
            match  e with
            | :? 'texn -> e
            | _ -> failtestf "Wrong exception type: %s" (e.GetType().FullName)

    let throwsWithMessage<'texn when 'texn :> exn> message f =
        throwsT2<'texn> f
        |> (fun (e : Exception) -> e.Message)
        |> Expect.equal "" message