namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open pulsar.proto
open Pulsar.Client.Common
open Pulsar.Client.Internal
open System
open Microsoft.Extensions.Logging
open System.Collections.Generic
open System.Timers
open System.IO

type Producer private (producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, lookup: BinaryLookupService) as this =
    let producerId = Generators.getNextProducerId()

    let prefix = sprintf "producer(%u, %s)" %producerId producerConfig.ProducerName
    let producerCreatedTsc = TaskCompletionSource<Producer>()
    let pendingMessages = Queue<PendingMessage>()
    let createProducerTimeout = DateTime.Now.Add(clientConfig.OperationTimeout)
    let sendTimeoutMs = producerConfig.SendTimeout.TotalMilliseconds
    let connectionHandler =
        ConnectionHandler(prefix,
                          lookup,
                          producerConfig.Topic.CompleteTopicName,
                          (fun () -> this.Mb.Post(ProducerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ProducerMessage.ConnectionFailed ex)),
                          Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        Max = TimeSpan.FromSeconds(60.0)
                                        MandatoryStop = TimeSpan.FromMilliseconds(Math.Max(100.0, sendTimeoutMs - 100.0))})


    let failPendingMessages (ex: exn) =
        while pendingMessages.Count > 0 do
            let msg = pendingMessages.Dequeue()
            msg.Tcs.SetException(ex)
        // TODO fail batch messages

    let timer = new Timer(sendTimeoutMs)

    let startSendTimeoutTimer () =
        if sendTimeoutMs > 0.0 then
            timer.AutoReset <- true
            timer.Elapsed.Add(fun _ -> this.Mb.Post TimeoutCheck)
            timer.Start()

    let resendMessages () =
        if pendingMessages.Count > 0 then
            Log.Logger.LogInformation("{0} resending {1} pending messages", prefix, pendingMessages.Count)
            while pendingMessages.Count > 0 do
                let pendingMessage = pendingMessages.Dequeue()
                this.Mb.Post(SendMessage pendingMessage)
        else
            producerCreatedTsc.TrySetResult(this) |> ignore

    let verifyIfLocalBufferIsCorrupted (msg: PendingMessage) =
        task {
            use stream = MemoryStreamManager.GetStream()
            use reader = new BinaryReader(stream)
            do! msg.Payload (stream :> Stream) // materialize stream
            let streamSize = stream.Length
            stream.Seek(4L, SeekOrigin.Begin) |> ignore
            let cmdSize = reader.ReadInt32() |> int32FromBigEndian
            stream.Seek((10+cmdSize) |> int64, SeekOrigin.Begin) |> ignore
            let checkSum = reader.ReadInt32() |> int32FromBigEndian
            let checkSumPayload = (int streamSize) - 14 - cmdSize
            let computedCheckSum = CRC32C.Get(0u, stream, checkSumPayload) |> int32
            return checkSum <> computedCheckSum
        }

    let mb = MailboxProcessor<ProducerMessage>.Start(fun inbox ->

        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with

                | ProducerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting register to topic {1}", prefix, producerConfig.Topic)
                        clientCnx.AddProducer producerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        try
                            let payload = Commands.newProducer producerConfig.Topic.CompleteTopicName producerConfig.ProducerName producerId requestId
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            let success = response |> PulsarResponseType.GetProducerSuccess
                            Log.Logger.LogInformation("{0} registered with name {1}", prefix, success.GeneratedProducerName)
                            connectionHandler.ResetBackoff()
                            resendMessages()
                        with
                        | ex ->
                            clientCnx.RemoveProducer producerId
                            Log.Logger.LogError(ex, "{0} Failed to create", prefix)
                            match ex with
                            | ProducerBlockedQuotaExceededException reason ->
                                Log.Logger.LogWarning("{0} Topic backlog quota exceeded. {1}", prefix, reason)
                                failPendingMessages(ex)
                            | ProducerBlockedQuotaExceededError reason ->
                                Log.Logger.LogWarning("{0} is blocked on creation because backlog exceeded. {1}", prefix, reason)
                            | _ ->
                                ()

                            match ex with
                            | TopicTerminatedException reason ->
                                connectionHandler.Terminate()
                                failPendingMessages(ex)
                                producerCreatedTsc.TrySetException(ex) |> ignore
                            | _ when producerCreatedTsc.Task.IsCompleted || (connectionHandler.IsRetriableError ex && DateTime.Now < createProducerTimeout) ->
                                // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                                // still within the initial timeout budget and we are dealing with a retriable error
                                connectionHandler.ReconnectLater ex
                            | _ ->
                                connectionHandler.Failed()
                                producerCreatedTsc.SetException(ex)
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)

                | ProducerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    let clientCnx = clientCnx :?> ClientCnx
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveProducer(producerId)

                | ProducerMessage.ConnectionFailed  ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > createProducerTimeout && producerCreatedTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()

                | ProducerMessage.BeginSendMessage (message, channel) ->

                    Log.Logger.LogDebug("{0} BeginSendMessage", prefix)
                    let sequenceId = Generators.getNextSequenceId()
                    let metadata =
                        MessageMetadata (
                            SequenceId = %sequenceId,
                            PublishTime = (DateTimeOffset.UtcNow.ToUnixTimeSeconds() |> uint64),
                            ProducerName = producerConfig.ProducerName,
                            UncompressedSize = (message.Length |> uint32)
                        )
                    let payload = Commands.newSend producerId sequenceId 1 metadata message
                    let tcs = TaskCompletionSource()
                    this.Mb.Post(SendMessage { SequenceId = sequenceId; Payload = payload; Tcs = tcs; CreatedAt = DateTime.Now })
                    channel.Reply(tcs)

                | ProducerMessage.SendMessage pendingMessage ->

                    Log.Logger.LogDebug("{0} SendMessage id={1}", prefix, %pendingMessage.SequenceId)
                    if pendingMessages.Count <= producerConfig.MaxPendingMessages then
                        pendingMessages.Enqueue(pendingMessage)
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let! success = clientCnx.Send pendingMessage.Payload
                            if success then
                                Log.Logger.LogDebug("{0} send complete", prefix)
                            else
                                Log.Logger.LogInformation("{0} send failed", prefix)
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    else
                        pendingMessage.Tcs.SetException(ProducerQueueIsFullError "Producer send queue is full.")

                | ProducerMessage.SendReceipt receipt ->

                    let sequenceId = receipt.SequenceId
                    let pendingMessage = pendingMessages.Peek()
                    let expectedSequenceId = pendingMessage.SequenceId
                    if sequenceId > expectedSequenceId then
                        Log.Logger.LogWarning(
                            "{0} Got ack for msg {1}. expecting {2} - got: {3} - queue-size: {4}",
                            prefix, receipt.MessageId, expectedSequenceId, sequenceId, pendingMessages.Count)
                        // Force connection closing so that messages can be re-transmitted in a new connection
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx -> clientCnx.Close()
                        | _ -> ()
                    elif sequenceId < expectedSequenceId then
                        Log.Logger.LogInformation(
                            "{0} Got ack for timed out msg {1} seq {2} last-seq: {3}",
                            prefix, receipt.MessageId, sequenceId, expectedSequenceId)
                    else
                        Log.Logger.LogDebug(
                            "{0} Received ack for message {1} sequenceId={2}",
                            prefix, receipt.MessageId, sequenceId)
                        pendingMessage.Tcs.SetResult(receipt.MessageId)
                        pendingMessages.Dequeue() |> ignore

                | ProducerMessage.RecoverChecksumError sequenceId ->

                    //* Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
                    //* message header-payload again.
                    //* <ul>
                    //* <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
                    //* message</li>
                    //* <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
                    //* So, fail send-message by failing callback</li>
                    //* </ul>
                    Log.Logger.LogWarning("{0} RecoverChecksumError id={1}", prefix, sequenceId)
                    if pendingMessages.Count > 0 then
                        let pendingMessage = pendingMessages.Peek()
                        let expectedSequenceId = pendingMessage.SequenceId
                        if sequenceId = expectedSequenceId then
                            let! corrupted = verifyIfLocalBufferIsCorrupted pendingMessage |> Async.AwaitTask
                            if corrupted then
                                // remove message from pendingMessages queue and fail callback
                                pendingMessages.Dequeue() |> ignore
                                pendingMessage.Tcs.SetException(ChecksumException "Checksum failed on corrupt message")
                            else
                                Log.Logger.LogDebug("{0} Message is not corrupted, retry send-message with sequenceId {1}", prefix, sequenceId)
                                resendMessages()
                        else
                            Log.Logger.LogDebug("{0} Corrupt message is already timed out {1}", prefix, sequenceId)
                    else
                        Log.Logger.LogDebug("{0} Got send failure for timed out seqId {1}", prefix, sequenceId)

                | ProducerMessage.Terminated ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        connectionHandler.Terminate()
                        failPendingMessages(TopicTerminatedException("The topic has been terminated"))

                | ProducerMessage.TimeoutCheck ->

                    match connectionHandler.ConnectionState with
                    | Closed | Terminated -> ()
                    | _ ->
                        if pendingMessages.Count > 0 then
                            let firstMessage = pendingMessages.Peek()
                            if firstMessage.CreatedAt.AddMilliseconds(sendTimeoutMs) >= DateTime.Now then
                                // The diff is less than or equal to zero, meaning that the message has been timed out.
                                // Set the callback to timeout on every message, then clear the pending queue.
                                Log.Logger.LogInformation("{0} Message send timed out. Failing {1} messages", prefix, pendingMessages.Count)
                                let ex = TimeoutException "Could not send message to broker within given timeout"
                                failPendingMessages ex

                | ProducerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        // TODO failPendingReceive
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseProducer producerId requestId
                        task {
                            try
                                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                                response |> PulsarResponseType.GetEmpty
                                clientCnx.RemoveProducer(producerId)
                                connectionHandler.Closed()
                                timer.Stop()
                                Log.Logger.LogInformation("{0} closed", prefix)
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} failed to close", prefix)
                                reraize ex
                        } |> channel.Reply
                    | _ ->
                        Log.Logger.LogInformation("{0} can't close since connection already closed", prefix)
                        connectionHandler.Closed()
                        timer.Stop()
                        channel.Reply(Task.FromResult())

                return! loop ()
            }
        loop ()
    )

    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))
    do startSendTimeoutTimer()

    member this.SendAndWaitAsync (msg: byte[]) =
        task {
            let! tcs = mb.PostAndAsyncReply(fun channel -> BeginSendMessage (msg, channel))
            return! tcs.Task
        }

    member this.SendAsync (msg: byte[]) =
        task {
            mb.PostAndAsyncReply(fun channel -> BeginSendMessage (msg, channel)) |> ignore
        }

    member this.CloseAsync() =
        task {
            let! result = mb.PostAndAsyncReply(ProducerMessage.Close)
            return! result
        }

    member private this.InitInternal() =
       task {
           do connectionHandler.GrabCnx()
           return! producerCreatedTsc.Task
       }

    member private this.Mb with get(): MailboxProcessor<ProducerMessage> = mb

    static member Init(producerConfig: ProducerConfiguration, clientConfig: PulsarClientConfiguration, lookup: BinaryLookupService) =
        task {
            let producer = new Producer(producerConfig, clientConfig, lookup)
            return! producer.InitInternal()
        }
