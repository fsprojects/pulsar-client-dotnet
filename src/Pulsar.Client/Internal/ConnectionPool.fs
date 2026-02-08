namespace Pulsar.Client.Internal

open Pulsar.Client.Common

open System.Collections.Concurrent
open System.Net
open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open Microsoft.Extensions.Logging
open Pulsar.Client.Internal
open System.IO.Pipelines
open Pulsar.Client.Api
open System.Net.Sockets
open System.Net.Security
open System.Security.Cryptography.X509Certificates

type internal ConnectionPool (config: PulsarClientConfiguration) =

    // Maximum number of retry attempts when a faulted connection is detected
    let maxConnectionRetries = 3
    let connections = ConcurrentDictionary<LogicalAddress, Lazy<Task<ClientCnx>>>()

    // from https://github.com/mgravell/Pipelines.Sockets.Unofficial/blob/master/src/Pipelines.Sockets.Unofficial/SocketConnection.Connect.cs
    let getSocket (endpoint: DnsEndPoint) =
        let addressFamily =
            if endpoint.AddressFamily = AddressFamily.Unspecified then
                AddressFamily.InterNetwork
            else
                endpoint.AddressFamily
        let protocolType =
            if addressFamily = AddressFamily.Unix then
                ProtocolType.Unspecified
            else
                ProtocolType.Tcp
        let socket = new Socket(addressFamily, SocketType.Stream, protocolType)
        SocketConnection.SetRecommendedClientOptions(socket)

        use args = new SocketAwaitableEventArgs(PipeScheduler.ThreadPool)
        args.RemoteEndPoint <- endpoint
        Log.Logger.LogDebug("Socket connecting to {0}", endpoint)

        backgroundTask {
            try
                if socket.ConnectAsync args |> not then
                    args.Complete()
                let! _ = args
                Log.Logger.LogDebug("Socket connected to {0}", endpoint)
                return socket
            with
            | Flatten ex ->
                socket.Dispose()
                Log.Logger.LogError(ex, "Socket connection failed to {0}", endpoint)
                return reraize ex
        }

    let getChainStatusString (chainStatuses: X509ChainStatus[]) =
        chainStatuses
        |> Seq.map (fun cs -> string cs.Status)
        |> String.concat "|"

    let remoteCertificateValidationCallback (_: obj) (cert: X509Certificate) (_: X509Chain) (errors: SslPolicyErrors) =
        let CheckRemoteCertWithTrustCertificate() =
            if isNull config.TlsTrustCertificate then
                false
            else
                let chain = new X509Chain()
                chain.ChainPolicy.VerificationFlags <-
                    X509VerificationFlags.AllowUnknownCertificateAuthority +
                    X509VerificationFlags.IgnoreEndRevocationUnknown +
                    X509VerificationFlags.IgnoreCertificateAuthorityRevocationUnknown +
                    X509VerificationFlags.IgnoreRootRevocationUnknown
                chain.ChainPolicy.RevocationMode <- X509RevocationMode.NoCheck
                let ca = config.TlsTrustCertificate
                chain.ChainPolicy.ExtraStore.Add ca |> ignore
                use cert2 = new X509Certificate2(cert)
                if chain.Build(cert2) then
                    let lastChainElm = chain.ChainElements[chain.ChainElements.Count - 1]
                    if lastChainElm.ChainElementStatus.Length = 1 then
                        let status = lastChainElm.ChainElementStatus[0].Status
                        if status = X509ChainStatusFlags.UntrustedRoot then
                            if System.Linq.Enumerable.SequenceEqual(lastChainElm.Certificate.RawData, ca.RawData) then
                                Log.Logger.LogDebug("Used TlsTrustCertificate")
                                true
                            else
                                Log.Logger.LogError("TlsTrustCertificate no equal with root certificate")
                                false
                        else
                            Log.Logger.LogError("Root certificate status {0} no equal UntrustedRoot", status)
                            false
                    else
                        let statusList = getChainStatusString chain.ChainStatus
                        Log.Logger.LogError("Building X.509 chain should have single status, statuses: [{0}]", statusList)
                        false
                else
                    let statusList = getChainStatusString chain.ChainStatus
                    Log.Logger.LogError("Building X.509 chain has failed, statuses: [{0}]", statusList)
                    false

        match errors with
        | SslPolicyErrors.None ->
            Log.Logger.LogDebug("No certificate errors")
            true
        | SslPolicyErrors.RemoteCertificateNotAvailable ->
            Log.Logger.LogError("RemoteCertificateNotAvailable")
            false
        | SslPolicyErrors.RemoteCertificateNameMismatch ->
            let result = not config.TlsHostnameVerificationEnable
            let log = if result then Log.Logger.LogWarning else Log.Logger.LogError
            log("RemoteCertificateNameMismatch")
            result
        | SslPolicyErrors.RemoteCertificateChainErrors ->
            let result = config.TlsAllowInsecureConnection || CheckRemoteCertWithTrustCertificate()
            let log = if result then Log.Logger.LogWarning else Log.Logger.LogError
            log("RemoteCertificateChainErrors")
            result
        | error when error = SslPolicyErrors.RemoteCertificateChainErrors + SslPolicyErrors.RemoteCertificateNameMismatch ->
            let result =
                not config.TlsHostnameVerificationEnable &&
                (config.TlsAllowInsecureConnection || CheckRemoteCertWithTrustCertificate())
            let log = if result then Log.Logger.LogWarning else Log.Logger.LogError
            log("RemoteCertificateChainErrors + RemoteCertificateNameMismatch")
            result
        | error ->
            Log.Logger.LogError("Unknown ssl error: {0}", error)
            false

    let unregisterClientCnx (broker: Broker) =
        let key = broker.LogicalAddress
        match connections.TryRemove(key) with
        | true, _ ->  Log.Logger.LogInformation("Connection backgroundTask {0} removed", key)
        | false, _ -> Log.Logger.LogDebug("Connection backgroundTask {0} was not removed", key)

    let rec connect (broker: Broker, maxMessageSize: int) =
        Log.Logger.LogInformation("Connecting to {0} with maxMessageSize: {1}",
                                  broker, maxMessageSize)
        backgroundTask {
            let (PhysicalAddress physicalAddress) = broker.PhysicalAddress
            // We should use PipeOptions.Default.PauseWriterThreshold as the minimum value for pauseWriterThreshold. A value that's too small isn't practical and will affect performance.
            let pauseWriterThreshold = max (int64 maxMessageSize) PipeOptions.Default.PauseWriterThreshold
            // Make sure the resumeWriterThreshold is half of the pauseWriterThreshold for better performance: https://github.com/dotnet/runtime/blob/970070e3b78b8cf604dabfe114e1f609c38c555d/src/libraries/System.IO.Pipelines/src/System/IO/Pipelines/PipeOptions.cs#L50-L51
            let resumeWriterThreshold = int64 (pauseWriterThreshold / 2L)
            let pipeOptions = PipeOptions(pauseWriterThreshold = pauseWriterThreshold, resumeWriterThreshold = resumeWriterThreshold )
            let! socket = getSocket physicalAddress

            try
                let! connection =
                    backgroundTask {
                        if config.UseTls then
                            Log.Logger.LogDebug("Configuring ssl for {0}", physicalAddress)
                            let sslStream = new SslStream(new NetworkStream(socket), false, RemoteCertificateValidationCallback(remoteCertificateValidationCallback))
                            let authData = config.Authentication.GetAuthData(physicalAddress.Host)
                            let clientCertificates =
                                if authData.HasDataForTls() then
                                    config.Authentication.GetAuthData(physicalAddress.Host).GetTlsCertificates()
                                else
                                    let clientCert = config.TlsCertificate
                                    if clientCert = null then
                                        X509Certificate2Collection()
                                    elif not clientCert.HasPrivateKey then
                                        failwith "TlsCertificate doesn't contain a private key"
                                    else
                                        X509Certificate2Collection([| clientCert |])
                            do! sslStream.AuthenticateAsClientAsync(physicalAddress.Host, clientCertificates, config.TlsProtocols, false)

                            let pipeConnection = StreamConnection.GetDuplex(sslStream, pipeOptions)
                            return
                                {
                                    Input = pipeConnection.Input
                                    Output = pipeConnection.Output
                                    Dispose = sslStream.Dispose
                                }
                        else
                            let pipeConnection = SocketConnection.Create(socket, pipeOptions)
                            return
                                {
                                    Input = pipeConnection.Input
                                    Output = pipeConnection.Output
                                    Dispose = pipeConnection.Dispose
                                }
                    }
                Log.Logger.LogDebug("Connection established for {0}", physicalAddress)
                let initialConnectionTsc = TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously)

                let clientCnx = ClientCnx(config, broker, connection, maxMessageSize, initialConnectionTsc,
                                          unregisterClientCnx)
                let connectPayload = clientCnx.NewConnectCommand()
                let! success = clientCnx.Send connectPayload
                if not success then
                    clientCnx.Dispose()
                    raise (ConnectionFailedOnSend "ConnectionPool connect")
                Log.Logger.LogInformation("Connected to {0}", broker)
                return! initialConnectionTsc.Task
            with Flatten ex ->
                try socket.Shutdown(SocketShutdown.Receive) with _ -> ()
                try socket.Shutdown(SocketShutdown.Send) with _ -> ()
                try socket.Close() with _ -> ()
                try socket.Dispose() with _ -> ()
                match ex with
                | MaxMessageSizeChanged newSize ->
                    Log.Logger.LogInformation("MaxMessageSizeChanged to {0}", newSize)
                    return! connect(broker, newSize)
                | _ ->
                    return reraize ex
        }

    member this.GetConnection (broker: Broker, maxMessageSize: int) =
        let mutable attemptNumber = 1
        let mutable result = None
        
        while result.IsNone && attemptNumber <= maxConnectionRetries do
            let t = connections.GetOrAdd(broker.LogicalAddress, fun _ ->
                    lazy connect(broker, maxMessageSize)).Value
            if t.IsFaulted then
                let key = broker.LogicalAddress
                // Try to remove the faulted connection and check if we actually removed it
                match connections.TryRemove(key) with
                | true, removedLazy when obj.ReferenceEquals(removedLazy.Value, t) ->
                    Log.Logger.LogInformation("Removed faulted connection task to {0}, attempt {1} of {2}", key, attemptNumber, maxConnectionRetries)
                    // Retry getting connection after removing faulted task
                    // Each retry will create a new connection with fresh DNS resolution
                    // No explicit delay is needed as GetOrAdd will trigger async DNS resolution and connection establishment
                    if attemptNumber < maxConnectionRetries then
                        attemptNumber <- attemptNumber + 1
                    else
                        Log.Logger.LogError("Failed to get connection to {0} after {1} attempts, returning faulted task", key, attemptNumber)
                        result <- Some t
                | true, _ ->
                    Log.Logger.LogDebug("Removed a connection to {0} but it was not the expected faulted task (race condition), retrying", key)
                    // Another thread replaced the connection, try again
                    attemptNumber <- attemptNumber + 1
                | false, _ -> 
                    Log.Logger.LogDebug("Faulted connection task to {0} wasn't removed (possibly already replaced)", key)
                    // Try again - another thread may have already fixed the connection
                    attemptNumber <- attemptNumber + 1
            else
                if attemptNumber > 1 then
                    Log.Logger.LogInformation("Successfully obtained connection to {0} on attempt {1}", broker.LogicalAddress, attemptNumber)
                result <- Some t
        
        // This should always be Some at this point, but handle the edge case
        match result with
        | Some t -> t
        | None -> 
            // Fallback: get whatever is in the cache or create new
            connections.GetOrAdd(broker.LogicalAddress, fun _ ->
                lazy connect(broker, maxMessageSize)).Value

    member this.GetBasicConnection (address: DnsEndPoint) =
        this.GetConnection({ LogicalAddress = LogicalAddress address; PhysicalAddress = PhysicalAddress address },
                           Commands.DEFAULT_MAX_MESSAGE_SIZE)

    member this.CloseAsync() =
        backgroundTask {
            for KeyValue(_, connectionTask) in connections do
                try
                    let! cnx = connectionTask.Value
                    cnx.Dispose()
                with ex ->
                    Log.Logger.LogError(ex, "Couldn't get connection on close")
                    ()
        }