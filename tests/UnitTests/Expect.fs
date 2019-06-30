namespace Pulsar.Client.UnitTests

open Expecto.Tests

module internal Expect =

    let throwsWithMessage<'texn> expectedMessage f =
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
        | Some e when e.Message <> expectedMessage ->
            failtestf "Expected f to throw an exn with message '%s', but exn with message '%s'was thrown."
                expectedMessage
                e.Message
        | Some _ -> ()
        | _ -> failtestf "%s. Expected f to throw." expectedMessage