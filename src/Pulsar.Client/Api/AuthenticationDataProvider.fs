namespace Pulsar.Client.Api

open System.Collections.Generic
open Pulsar.Client.Common
open System.Text
open System.Security.Cryptography.X509Certificates

type AuthenticationDataProvider() =

    //TLS
    
    abstract member HasDataForTls: unit -> bool
    default this.HasDataForTls() =
        false
    
    abstract member GetTlsCertificates: unit -> X509CertificateCollection
    default this.GetTlsCertificates() =
        null

    //Command
    
    abstract member HasDataFromCommand: unit -> bool
    default this.HasDataFromCommand() =
        false

    abstract member GetCommandData: unit -> string
    default this.GetCommandData() =
        ""

    //Http

    abstract member HasDataForHttp: unit -> bool
    default this.HasDataForHttp() =
        false

    abstract member GetHttpHeaders: unit -> IReadOnlyDictionary<string, string>
    default this.GetHttpHeaders() =
        Dictionary<string, string>()

    //AuthDataProvider

    abstract member Authenticate: AuthData -> AuthData
    default this.Authenticate authData =
        let bytes =
            if this.HasDataFromCommand() then this.GetCommandData() else ""
            |> Encoding.UTF8.GetBytes
        { Bytes = bytes }
