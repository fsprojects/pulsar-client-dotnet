namespace Pulsar.Client.Auth

open System.IO
open System.Security.Cryptography.X509Certificates
open Pulsar.Client.Api

type internal AuthenticationDataTls(certFile: FileInfo) =
    inherit AuthenticationDataProvider()
    
    let readCertificate() =
        let cert = new X509Certificate2(certFile.FullName)
        if cert.HasPrivateKey then
            cert :> X509Certificate
        else
            cert.Dispose()
            failwith "Certificate doesn't contain a private key"

    let mutable certFileLastWriteTime = certFile.LastWriteTime

    let mutable certificate = readCertificate()
            
    let checkAndRefresh() =
        certFile.Refresh()
        if certFileLastWriteTime <> certFile.LastWriteTime then
            certFileLastWriteTime <- certFile.LastWriteTime
            certificate.Dispose()
            certificate <- readCertificate()
            
    override this.HasDataForTls() =
        true
        
    override this.GetTlsCertificates() =
        checkAndRefresh()
        X509CertificateCollection([|certificate|])


