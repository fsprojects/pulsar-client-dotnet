namespace Pulsar.Client.Common

type NamespaceName(name : string) =

    let tenant, localName =
        name
        |> invalidArgIfBlankString "Namespace name must not be blank."
        |> fun text -> text.Split('/')
        |> invalidArgIf
            (fun ar -> ar.Length <> 2)
            "Invalid namespace name format. Namespace name must be specified as '<tenant>/<namespace>'."
        |> fun ar -> (ar.[0], ar.[1])

    member this.Tenant = tenant

    member this.LocalName = localName
    
    static member SYSTEM_NAMESPACE = NamespaceName("pulsar/system")

    override this.ToString() =
        name