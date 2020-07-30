namespace Pulsar.Client.Api

open Pulsar.Client.Common

type IMessageEncrypt =

    abstract member Encrypt: payload: byte [] -> EncryptedMessage

    abstract member UpdateEncryptionKeys: unit -> unit

type IMessageDecrypt =
    
    abstract member Decrypt: EncryptedMessage -> byte []
    