module Pulsar.Client.IntegrationTests.ProducerBatchOrdering

open System
open System.Net.Http
open System.Text
open System.Threading
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common

type private Delivery =
    | Immediate
    | After of TimeSpan

type private Deduplication =
    | Enabled
    | Disabled

let private createNamespace (http: HttpClient) namespaceName =
    task {
        use body = new StringContent("{\"replication_clusters\":[\"standalone\"]}", Encoding.UTF8, "application/json")
        use! response = http.PutAsync($"/admin/v2/namespaces/{namespaceName}", body)
        response.EnsureSuccessStatusCode() |> ignore

        return
            { new IAsyncDisposable with
                member _.DisposeAsync() =
                    ValueTask(task {
                        use! response = http.DeleteAsync($"/admin/v2/namespaces/{namespaceName}?force=true")
                        response.EnsureSuccessStatusCode() |> ignore
                    }) }
    }

let private checkBatchOrdering builder keys delivery deduplication =
    task {
        let namespaceName = "public/batch-ordering-" + Guid.NewGuid().ToString("N")
        let topicPath = namespaceName + "/messages"
        let topicName = "persistent://" + topicPath
        use http = new HttpClient(new HttpClientHandler(UseProxy = false))
        http.BaseAddress <- Uri(pulsarHttpAddress)
        http.Timeout <- TimeSpan.FromSeconds(15.0)
        use! namespaceScope = createNamespace http namespaceName

        use! topicResponse = http.PutAsync($"/admin/v2/persistent/{topicPath}", null)
        topicResponse.EnsureSuccessStatusCode() |> ignore
        let deduplicationJson =
            match deduplication with
            | Enabled -> "true"
            | Disabled -> "false"
        use dedupBody = new StringContent(deduplicationJson, Encoding.UTF8, "application/json")
        use! dedupResponse = http.PostAsync($"/admin/v2/persistent/{topicPath}/deduplicationEnabled", dedupBody)
        dedupResponse.EnsureSuccessStatusCode() |> ignore

        let client = getClient()
        // Exclusive readers audit persistence without waiting for delayed delivery.
        use! reader =
            client.NewReader()
                .Topic(topicName)
                .StartMessageId(MessageId.Earliest)
                .CreateAsync()
        use! producer =
            client.NewProducer()
                .Topic(topicName)
                .ProducerName("batch-ordering")
                .EnableBatching(true)
                .BatchBuilder(builder)
                .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1.0))
                .SendTimeout(TimeSpan.FromSeconds(10.0))
                .CreateAsync()

        let expected = ResizeArray<string * string * SequenceId>()
        for round in 0 .. 9 do
            let sends = ResizeArray<Task<MessageId>>()
            let submit key delivery =
                let value = $"round-{round}-message-{expected.Count}"
                let sequenceId: SequenceId = %(int64 expected.Count)
                expected.Add(value, key, sequenceId)
                let deliverAt =
                    match delivery with
                    | Immediate -> Nullable<TimeStamp>()
                    | After delay -> Nullable<TimeStamp>(%DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds())
                producer.NewMessage(Encoding.UTF8.GetBytes(value), key = key, deliverAt = deliverAt)
                |> producer.SendAsync |> sends.Add
            for key in keys do
                submit key Immediate
            match delivery with
            | Immediate -> ()
            | After _ -> submit "D" delivery
            // Await only after flushing so ordinary messages can accumulate in the same batch.
            let flush = producer.FlushAsync()
            let allSends = Task.WhenAll(sends)
            do! Task.WhenAll(allSends :> Task, flush).WaitAsync(TimeSpan.FromSeconds(20.0))

        let received = ResizeArray<string * string * SequenceId>()
        let mutable available = true
        while available do
            let! hasMessage = reader.HasMessageAvailableAsync().WaitAsync(TimeSpan.FromSeconds(10.0))
            available <- hasMessage
            if available then
                use timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10.0))
                let! message = reader.ReadNextAsync(timeout.Token)
                received.Add(Encoding.UTF8.GetString(message.Data), %message.Key, message.SequenceId)

        let expectedValues = expected |> Seq.map (fun (value, _, _) -> value) |> Set.ofSeq
        let receivedValues = received |> Seq.map (fun (value, _, _) -> value) |> Set.ofSeq
        Expect.equal receivedValues expectedValues "Every submitted payload must be persisted"
        Expect.equal received.Count receivedValues.Count "Each payload must be received exactly once"
        Expect.equal (received |> Set.ofSeq) (expected |> Set.ofSeq) "Keys and per-message sequence IDs must be preserved"
        for key in expected |> Seq.map (fun (_, key, _) -> key) |> Seq.distinct do
            let forKey items = items |> Seq.filter (fun (_, k, _) -> k = key) |> Seq.toList
            Expect.equal (forKey received) (forKey expected) $"Messages for key {key} must retain their order"
        let! lastSequenceId = producer.LastSequenceId()
        Expect.equal lastSequenceId (%(int64 expected.Count - 1L)) "LastSequenceId must cover all submitted messages"
    }

[<Tests>]
let tests =

    testList "ProducerBatchOrdering" [

        testTask "Default with deliverAt" {
            do! checkBatchOrdering BatchBuilder.Default ["A"] (After (TimeSpan.FromMinutes(1.0))) Enabled
        }

        testTask "KeyBased interleaved" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "A"; "B"] Immediate Enabled
        }

        testTask "KeyBased highest sequence sorting" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "B"; "A"] Immediate Enabled
        }

        testTask "KeyBased grouped" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "A"; "B"; "B"] Immediate Enabled
        }

        testTask "KeyBased singleton and batch" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "A"] Immediate Enabled
        }

        testTask "KeyBased interleaved with deliverAt" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "A"; "B"] (After (TimeSpan.FromMinutes(1.0))) Enabled
        }

        testTask "KeyBased sorting with deliverAt" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "B"; "A"] (After (TimeSpan.FromMinutes(1.0))) Enabled
        }

        testTask "Default interleaved" {
            do! checkBatchOrdering BatchBuilder.Default ["A"; "B"; "A"; "B"] Immediate Enabled
        }

        testTask "KeyBased without dedup" {
            do! checkBatchOrdering BatchBuilder.KeyBased ["A"; "B"; "A"; "B"] Immediate Disabled
        }
    ]
