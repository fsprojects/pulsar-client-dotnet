namespace Pulsar.Client.Api

open System
open System.Security.Cryptography.X509Certificates
open System.Threading.Tasks

type IServiceUrlProviderContext =
    abstract member UpdateServiceUrl: string -> Task<unit>
    abstract member UpdateAuthentication: Authentication -> unit
    abstract member UpdateTlsTrustCertificate: X509Certificate2 -> unit

type IServiceUrlProvider =
    inherit IDisposable
    abstract member Initialize: IServiceUrlProviderContext -> unit
    abstract member GetServiceUrl: unit -> string
