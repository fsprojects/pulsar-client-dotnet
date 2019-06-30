namespace Pulsar.Client.UnitTests

open System
open Expecto
open Expecto.Flip

module internal Expect =

    let throwsTh<'texn> f =
        let thrown =
            try
                f ()
                None
            with e ->
                Some e
        match thrown with
        | Some e when e.GetType() <> typeof<'texn> ->
            failtestf "Expected f to throw an exn of type %s, but one of type %s was thrown."
                (typeof<'texn>.FullName)
                (e.GetType().FullName)
        | Some e -> e
        | _ -> failtestf "Expected f to throw."

    let throwsWithMessage<'texn> message f =
        throwsTh<'texn> f
        |> (fun (e : Exception) -> e.Message)
        |> Expect.equal "" message