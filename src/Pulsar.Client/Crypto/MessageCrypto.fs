namespace Pulsar.Client.Crypto

open System
open System.IO
open System.Security.Cryptography
open Microsoft.Extensions.Caching.Memory
open NSec.Cryptography
open PemUtils
open Pulsar.Client.Api
open Pulsar.Client.Common
open Microsoft.Extensions.Logging

type MessageEncryptor(keyNames: string seq, keyReader: ICryptoKeyReader) =
    let symmetricAlgorithm = AeadAlgorithm.Aes256Gcm
    let IV_LEN = 12
    let symmetricAlgorithmName = symmetricAlgorithm.ToString()
    let mutable symmetricKey: Key = null
    let mutable encryptionKeys: EncryptionKey [] = null
    let generator = RandomNumberGenerator.Create()

    let createKey () =
        Log.Logger.LogDebug("MessageEncryptor create new symmetric key")
        let createParameters = KeyCreationParameters(ExportPolicy = KeyExportPolicies.AllowPlaintextExport)
        new Key(symmetricAlgorithm, &createParameters)

    let createNonce () =
        let bytes = Array.zeroCreate IV_LEN
        generator.GetBytes bytes
        bytes

    let parsePublicKey (rawKey: byte []) =
        use rawStream = new MemoryStream(rawKey, writable = false)
        use pemReader = new PemReader(rawStream)
        pemReader.ReadRsaKey()

    let loadPublicKey keyName =
        let publicKeyInfo = keyReader.GetPublicKey keyName
        let rsaPublicKey = parsePublicKey publicKeyInfo.Key
        use rsa = RSA.Create(rsaPublicKey)
        let encKey = rsa.Encrypt(symmetricKey.Export(KeyBlobFormat.RawSymmetricKey), RSAEncryptionPadding.OaepSHA1)
        let encryptionKeys = EncryptionKey(keyName, encKey, publicKeyInfo.Metadata)
        encryptionKeys

    let createEncryptionKeys () =
        keyNames
        |> Seq.map loadPublicKey
        |> Seq.toArray

    let init () =
        symmetricKey <- createKey ()
        encryptionKeys <- createEncryptionKeys ()

    do init ()

    interface IMessageEncryptor with

        member this.Encrypt(payload: byte []) =
            Log.Logger.LogDebug("MessageEncryptor encrypt payload")
            let nonce = createNonce()
            let encryptPayload = symmetricAlgorithm.Encrypt(symmetricKey, nonce, ReadOnlySpan.Empty, payload)
            EncryptedMessage(encryptPayload, encryptionKeys, symmetricAlgorithmName, nonce)

        member this.UpdateEncryptionKeys() =
            Log.Logger.LogDebug("MessageEncryptor update encryptionKeys")
            init()



type MessageDecryptor(keyReader: ICryptoKeyReader) =
    let symmetricAlgorithm = AeadAlgorithm.Aes256Gcm
    let symmetricKeysCache = new MemoryCache(MemoryCacheOptions(ExpirationScanFrequency = TimeSpan.FromHours(4.0)))

    let parsePrivateKey (rawKey: byte []) =
        use rawStream = new MemoryStream(rawKey, writable = false)
        use pemReader = new PemReader(rawStream)
        pemReader.ReadRsaKey()

    let tryDecryptSymmetricKey (encryptionKey: EncryptionKey) =
        Log.Logger.LogDebug("MessageDecryptor try decrypt symmetric key")
        let encKey = encryptionKey.Value
        try
            let privateKey = keyReader.GetPrivateKey(encryptionKey.Name, encryptionKey.Metadata)
            let rsaPrivateKey = parsePrivateKey privateKey.Key
            use rsa = RSA.Create(rsaPrivateKey)
            let keyBlob = rsa.Decrypt(encKey, RSAEncryptionPadding.OaepSHA1)
            Some(encKey, keyBlob)
        with ex ->
            Log.Logger.LogInformation("MessageDecryptor failed attempt to decrypt the symmetric key")
            None

    let tryGetKeyFromCache (encryptionKeys: EncryptionKey []) =
        encryptionKeys
        |> Array.tryPick (fun ek ->
            let cacheKey = Convert.ToBase64String(ek.Value)
            match symmetricKeysCache.TryGetValue<Key>(cacheKey) with
            | true, key -> Some key
            | _ -> None
            )

    let getSymmetricKey (encryptionKeys: EncryptionKey []) =
        match tryGetKeyFromCache encryptionKeys with
        | Some key ->
            Log.Logger.LogDebug("MessageDecryptor symmetric key found in cache")
            key
        | None ->
            let encKey, keyBlob =
                encryptionKeys
                |> Array.tryPick tryDecryptSymmetricKey
                |> Option.defaultWith(fun () ->
                    Log.Logger.LogError("MessageDecryptor failed to decrypt symmetric key")
                    raise <| CryptoException "Failed to decrypt symmetric key"
                    )
            let cacheKey = Convert.ToBase64String(encKey)
            let key = Key.Import(symmetricAlgorithm, keyBlob, KeyBlobFormat.RawSymmetricKey)
            Log.Logger.LogDebug("MessageDecryptor Symmetric key received from message and put in cache")
            symmetricKeysCache.Set<Key>(cacheKey, key)

    interface IMessageDecryptor with
        member this.Decrypt(encryptMessage: EncryptedMessage) =
            let symmetricKey = getSymmetricKey encryptMessage.EncryptionKeys
            let nonce = encryptMessage.EncryptionParam
            match symmetricAlgorithm.Decrypt(symmetricKey, nonce, ReadOnlySpan.Empty, encryptMessage.EncPayload) with
            | null ->
                raise <| CryptoException "An error occurred while decrypting the message"
            | decryptedMessage ->
                Log.Logger.LogDebug("MessageDecryptor Message successful decrypted")
                decryptedMessage
