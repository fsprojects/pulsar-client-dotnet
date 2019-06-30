namespace Pulsar.Client.Common

open Pulsar.Client.ExceptionHelper

type NamespaceName(name : string) =

    let tenant, localName =
        name
        |> invalidArgIfBlankString "Namespace name must not be blank."
        |> fun text -> text.Split('/')
        |> invalidArgIf
            (fun ar -> ar.Length <> 2)
            "Invalid namespace name format. Namespace name must be specified as '<tenant>/<namespace>'."
        |> fun ar -> (ar.[0], ar.[1])

    member __.Tenant
        with get() = tenant

    member __.LocalName
        with get() = localName

    override __.ToString() = name