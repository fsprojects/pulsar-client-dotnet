namespace Pulsar.Client.IntegrationTests

open System
open Expecto
open Expecto.Flip

module internal Expect =

    let throwsT2<'texn> f =
        let thrown =
            try
                f ()
                None
            with e ->
                Some e
        match thrown with
        | Some e  when  e.InnerException.GetType() = typeof<'texn> ->
            e
        | Some e when e.GetType() = typeof<AggregateException> && e.InnerException.GetType() = typeof<'texn>->
            e
        | Some e ->
            failtestf "Expected f to throw an exn of type %s, but one of type %s was thrown."
                (typeof<'texn>.FullName)
                (e.GetType().FullName)
        | _ -> failtestf "Expected f to throw."

    let throwsWithMessage<'texn> message f =
        throwsT2<'texn> f
        |> (fun (e : Exception) -> e.Message)
        |> Expect.equal "" message