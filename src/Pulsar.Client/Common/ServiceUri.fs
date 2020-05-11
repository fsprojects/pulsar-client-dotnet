namespace Pulsar.Client.Common

open System.Text.RegularExpressions
open System

type internal ServiceUri = {
    OriginalString : string
    Addresses : Uri list
    UseTls : bool
}

[<RequireQualifiedAccess>]
module internal ServiceUri =

    let private BINARY_SERVICE = "pulsar"
    let private SSL_SERVICE = "ssl"

    let private BINARY_PORT = 6650
    let private BINARY_TLS_PORT = 6651

    let private schemeGroup = "scheme"
    let private servicesGroup = "services"
    let private hostsGroup = "hosts"
    let private pathGroup = "path"

    let private pattern =
        sprintf
            "^(?<%s>pulsar)(?:\+(?<%s>ssl))*://(?:(?<%s>[^\s/;,]+)[;,]?)+(?<%s>/.+)?$"
            schemeGroup
            servicesGroup
            hostsGroup
            pathGroup

    let private regex = Regex(pattern, RegexOptions.Compiled)

    let private getGroupValue (name : string) (m : Match) =
        m.Groups.[name].Value

    let private getGroupCaptureValues (name : string) (m : Match) =
        m.Groups.[name].Captures |> Seq.cast<Capture> |> Seq.map (fun c -> c.Value)

    let parse str =

        if String.IsNullOrWhiteSpace(str) then
            Result.Error "ServiceUrl must not be blank."
        else
            let m = regex.Match(str)

            if m = Match.Empty then
                Result.Error (sprintf "Supplied string '%s' describe an un-representable ServiceUri." str)
            else
                let scheme = m |> getGroupValue schemeGroup
                let services = m |> getGroupCaptureValues servicesGroup
                let hosts = m |> getGroupCaptureValues hostsGroup
                let path = m |> getGroupValue pathGroup
                let useTls = services |> Seq.contains SSL_SERVICE && scheme = BINARY_SERVICE

                let createBuilder host = UriBuilder(sprintf "%s://%s%s" scheme host path)

                let rewritePort (builder : UriBuilder) =
                    if builder.Port = -1 then
                        if services |> Seq.contains SSL_SERVICE then
                            builder.Port <- BINARY_TLS_PORT
                        else
                            builder.Port <- BINARY_PORT
                    builder

                let dropUserInfo (builder : UriBuilder) =
                    builder.UserName <- null
                    builder.Password <- null
                    builder

                let getUri (builder : UriBuilder) = builder.Uri

                let addresses = hosts |> Seq.map (createBuilder >> rewritePort >> dropUserInfo >> getUri) |> List.ofSeq

                Ok { OriginalString = str; Addresses = addresses; UseTls = useTls }