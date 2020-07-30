namespace Pulsar.Client.Crypto

open System
open System.IO
open System.Security.Cryptography
open Microsoft.Extensions.Caching.Memory
open NSec.Cryptography
open PemUtils
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging

type MessageEncryptNSec(keyNames: string [], keyReader: ICryptoKeyReader) =
    let symmetricAlgorithm = AeadAlgorithm.Aes256Gcm
    let mutable symmetricKey: Key = null
    let mutable nonce = Nonce()
    let mutable encryptionKeys: EncryptionKeys [] = null

    let createKey () =
        Log.Logger.LogDebug("MessageEncryptNSec create new symmetric key")
        let createParameters = KeyCreationParameters(ExportPolicy = KeyExportPolicies.AllowPlaintextExport)
        new Key(symmetricAlgorithm, &createParameters)

    let createNonce () =
        Log.Logger.LogDebug("MessageEncryptNSec create new nonce")
        let fixPart = ReadOnlySpan(NSec.Cryptography.RandomGenerator.Default.GenerateBytes(4))
        Nonce(fixPart, 12 - fixPart.Length)

    let parsePublicKey (rawKey: byte []) =
        use rawStream = new MemoryStream(rawKey, writable = false)
        use pemReader = new PemReader(rawStream)
        pemReader.ReadRsaKey()

    let tryLoadPublicKey keyName =
        try
            let publicKey = keyReader.GetPublicKey(keyName)
            let rsaPublicKey = parsePublicKey (publicKey.Key)
            let rsa = RSA.Create()
            rsa.ImportParameters(rsaPublicKey)
            let encKey = rsa.Encrypt(symmetricKey.Export(KeyBlobFormat.RawSymmetricKey), RSAEncryptionPadding.OaepSHA1)
            let encryptionKeys = EncryptionKeys(keyName, encKey, publicKey.Metadata)
            encryptionKeys
        with ex ->
            Log.Logger.LogError("MessageEncryptNSec failed publicKey load", ex)
            raise <| CryptoException ex.Message
   
    let createEncryptionKeys () =
        Log.Logger.LogDebug("MessageEncryptNSec update encryptionKeys")
        keyNames
        |> Seq.map tryLoadPublicKey
        |> Seq.toArray

    let init () =
        symmetricKey <- createKey ()
        nonce <- createNonce ()
        encryptionKeys <- createEncryptionKeys ()

    do init ()

    interface IMessageEncrypt with

        member this.Encrypt(payload: byte []) =
            Log.Logger.LogDebug("MessageEncryptNSec encrypt payload")
            let encryptPayload =
                symmetricAlgorithm.Encrypt(symmetricKey, &nonce, ReadOnlySpan.Empty, ReadOnlySpan payload)
            EncryptedMessage(encryptPayload, encryptionKeys, nonce.ToArray())

        member this.UpdateEncryptionKeys() = init ()


type MessageDecryptNSec(keyReader: ICryptoKeyReader) =
    let symmetricAlgorithm = AeadAlgorithm.Aes256Gcm

    let symmetricKeysCache =
        let expiredTime = TimeSpan(hours = 4, minutes = 0, seconds = 0)
        new MemoryCache(MemoryCacheOptions(ExpirationScanFrequency = expiredTime))

    let parsePrivateKey (rawKey: byte []) =
        use rawStream = new MemoryStream(rawKey, writable = false)
        use pemReader = new PemReader(rawStream)
        pemReader.ReadRsaKey()

    let tryDecryptSymmetricKey (encryptionKey: EncryptionKeys) =
        Log.Logger.LogDebug("MessageDecryptNSec try decrypt symmetric key")
        let encKey = encryptionKey.Value
        try
            let privateKey = keyReader.GetPrivateKey(encryptionKey.Key, encryptionKey.Metadata)
            let rsaPrivateKey = parsePrivateKey (privateKey.Key)
            let rsa = RSA.Create()
            rsa.ImportParameters(rsaPrivateKey)
            let keyBlob = rsa.Decrypt(encKey, RSAEncryptionPadding.OaepSHA1)
            Some(encKey, keyBlob)
        with ex ->
            Log.Logger.LogInformation("MessageDecryptNSec failed attempt to decrypt the symmetric key")
            None

    let tryGetKeyFromCache (encryptionKeys: EncryptionKeys []) =
        encryptionKeys
        |> Seq.choose (fun ek ->
            let cacheKey = Convert.ToBase64String(ek.Value)
            match symmetricKeysCache.TryGetValue<Key>(cacheKey) with
            | true, key -> Some key
            | _ -> None)
        |> Seq.tryHead

    let getSymmetricKey (encryptionKeys: EncryptionKeys []) =
        match tryGetKeyFromCache encryptionKeys with
        | Some key ->
            Log.Logger.LogDebug("MessageDecryptNSec symmetric key found in cache")
            key
        | None ->
            let (encKey, keyBlob) =
                encryptionKeys
                |> Seq.choose tryDecryptSymmetricKey
                |> Seq.tryHead
                |> Option.defaultWith(fun () ->
                    Log.Logger.LogError("MessageDecryptNSec failed to decrypt symmetric key")
                    raise <| CryptoException "Failed to decrypt symmetric key"
                    )

            let cacheKey = Convert.ToBase64String(encKey)
            let key = Key.Import(symmetricAlgorithm, ReadOnlySpan keyBlob, KeyBlobFormat.RawSymmetricKey)
            Log.Logger.LogDebug("MessageDecryptNSec Symmetric key received from message and put in cache")
            symmetricKeysCache.Set<Key>(cacheKey, key)

    interface IMessageDecrypt with
        member this.Decrypt(encryptMessage: EncryptedMessage) =
            let symmetricKey = getSymmetricKey encryptMessage.EncryptionKeys
            let mutable nonce = Nonce(ReadOnlySpan encryptMessage.EncryptionParam, 0)

            match symmetricAlgorithm.Decrypt
                      (symmetricKey, &nonce, ReadOnlySpan.Empty, ReadOnlySpan encryptMessage.EncPayload) with
            | true, d ->
                Log.Logger.LogDebug("MessageDecryptNSec Message successful decrypted")
                d
            | _ -> raise <| CryptoException "An error occurred while decrypting the message"
