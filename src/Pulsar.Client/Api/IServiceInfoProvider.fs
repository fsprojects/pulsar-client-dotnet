namespace Pulsar.Client.Api

open System
open System.Security.Cryptography.X509Certificates
open System.Threading.Tasks
open Pulsar.Client.Common

type ServiceInfo(serviceUrl: string,
                 authentication: Authentication,
                 tlsTrustCertificate: X509Certificate2) =

    let parsed =
        match ServiceUri.parse serviceUrl with
        | Ok serviceUri -> serviceUri
        | Error message -> invalidArg "serviceUrl" message

    let authentication =
        authentication
        |> invalidArgIfDefault "authentication can't be null"

    member _.ServiceUrl = parsed
    member _.Authentication = authentication
    member _.TlsTrustCertificate = tlsTrustCertificate

    new(serviceUrl: string) =
        ServiceInfo(serviceUrl, Authentication.AuthenticationDisabled, null)

    new(serviceUrl: string, authentication: Authentication) =
        ServiceInfo(serviceUrl, authentication, null)

type IServiceInfoProviderContext =
    abstract member UpdateServiceInfo: ServiceInfo -> Task<unit>

type IServiceInfoProvider =
    inherit IDisposable
    abstract member Initialize: IServiceInfoProviderContext -> unit
    abstract member GetServiceInfo: unit -> ServiceInfo
