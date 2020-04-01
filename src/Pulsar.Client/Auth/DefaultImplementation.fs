module internal Pulsar.Client.Auth.DefaultImplementation

let newAuthenticationToken (token: string) =
    AuthenticationToken token

let newAuthenticationTls (certFilePath: string) =
    AuthenticationTls certFilePath