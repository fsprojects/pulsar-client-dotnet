namespace Pulsar.Client.Api

open Pulsar.Client.Common

type IMessageEncryptor =

    abstract member Encrypt: payload: byte [] -> EncryptedMessage

    abstract member UpdateEncryptionKeys: unit -> unit

type IMessageDecryptor =
    
    abstract member Decrypt: encryptedPayload: EncryptedMessage -> byte []
    