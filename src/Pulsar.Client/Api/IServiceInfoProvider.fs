namespace Pulsar.Client.Api

open System
open System.Security.Cryptography.X509Certificates
open System.Threading.Tasks
open Pulsar.Client.Common

type ServiceInfo(serviceUrl: ServiceUri,
                 authentication: Authentication,
                 tlsTrustCertificate: X509Certificate2) =
    let authentication =
        authentication
        |> invalidArgIfDefault "authentication can't be null"

    static member private Parse serviceUrl =
        match ServiceUri.parse serviceUrl with
        | Ok serviceUri -> serviceUri
        | Error message -> invalidArg "serviceUrl" message

    member _.ServiceUrl = serviceUrl
    member _.Authentication = authentication
    member _.TlsTrustCertificate = tlsTrustCertificate

    new(serviceUrl: string) =
        ServiceInfo(ServiceInfo.Parse serviceUrl, Authentication.AuthenticationDisabled, null)

    new(serviceUrl: string, authentication: Authentication) =
        ServiceInfo(ServiceInfo.Parse serviceUrl, authentication, null)

    new(serviceUrl: string, authentication: Authentication, tlsTrustCertificate: X509Certificate2) =
        ServiceInfo(ServiceInfo.Parse serviceUrl, authentication, tlsTrustCertificate)

type IServiceInfoProviderContext =
    abstract member UpdateServiceInfo: ServiceInfo -> Task<unit>

type IServiceInfoProvider =
    inherit IDisposable
    abstract member Initialize: IServiceInfoProviderContext -> unit
    abstract member GetServiceInfo: unit -> ServiceInfo
