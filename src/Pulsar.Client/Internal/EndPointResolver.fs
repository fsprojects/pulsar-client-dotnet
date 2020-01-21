namespace Pulsar.Client.Internal

open System
open System.Net

type internal EndPointResolver(addresses : Uri list) =

    do
        if List.isEmpty addresses then
            invalidArg "addresses" "Addresses list could not be empty."

    let mb = MailboxProcessor<AsyncReplyChannel<DnsEndPoint>>.Start(fun inbox ->

        let rec loop index (endPoints : DnsEndPoint list) = async {
            let! channel = inbox.Receive()
            let endPoint = endPoints.[index]
            let nextIndex = (index + 1) % endPoints.Length
            channel.Reply endPoint
            return! loop nextIndex endPoints 
        }

        addresses
        |> List.map (fun uri -> DnsEndPoint(uri.Host, uri.Port))
        |> loop 0
    )

    with
        member __.Resolve() = mb.PostAndReply(id)