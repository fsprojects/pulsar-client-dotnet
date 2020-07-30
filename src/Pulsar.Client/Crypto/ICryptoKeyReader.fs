namespace Pulsar.Client.Crypto

open System.Collections.Generic

/// Interface that abstracts the access to a key store.
[<AllowNullLiteral>]
type ICryptoKeyReader =
    
    abstract member GetPublicKey: keyName: string -> KeyInfo

    abstract member GetPrivateKey: keyName: string * metadata: Dictionary<string, string> -> KeyInfo
