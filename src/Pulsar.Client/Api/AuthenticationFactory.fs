namespace Pulsar.Client.Api

open Pulsar.Client.Auth

[<AbstractClass; Sealed>]
type AuthenticationFactory =

    static member Token (token: string) : Authentication =
        DefaultImplementation.newAuthenticationToken token :> Authentication

    static member Tls (certFilePath: string) : Authentication =
        DefaultImplementation.newAuthenticationTls certFilePath :> Authentication
    
