namespace Pulsar.Client.Crypto

open System.Collections.Generic

type EncryptionKeyInfo = {
    Key: byte[]
    Metadata: IReadOnlyDictionary<string, string>
}

/// Interface that abstracts the access to a key store.
type ICryptoKeyReader =
    
    abstract member GetPublicKey: keyName: string -> EncryptionKeyInfo

    abstract member GetPrivateKey: keyName: string * metadata: IReadOnlyDictionary<string, string> -> EncryptionKeyInfo
