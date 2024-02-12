namespace Pulsar.Client.Api

open System
open Microsoft.FSharp.Core
open Pulsar.Client.Auth

/// Factory class that allows to create Authentication instances for all the supported authentication methods.
[<AbstractClass; Sealed>]
type AuthenticationFactory =

    /// Create an authentication provider for token based authentication.
    static member Token (token: string) : Authentication =
        DefaultImplementation.newAuthenticationToken token :> Authentication

    /// Create an authentication provider for token based authentication.
    static member Token (tokenSupplier: Func<string>) : Authentication =
        DefaultImplementation.newAuthenticationTokenSupplier(FuncConvert.FromFunc tokenSupplier) :> Authentication

    /// Create an authentication provider for TLS based authentication.
    static member Tls (certFilePath: string) : Authentication =
        DefaultImplementation.newAuthenticationTls certFilePath :> Authentication

