module Pulsar.Client.UnitTests.Api.PulsarClientTests

open System
open System.Reflection
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common

let private newClient () =
    PulsarClient({ PulsarClientConfiguration.Default with ServiceAddresses = [| Uri("pulsar://localhost:6650") |] })

let private mailbox (client: PulsarClient) =
    typeof<PulsarClient>.GetProperty("Mb", BindingFlags.Instance ||| BindingFlags.NonPublic).GetValue(client)
    :?> Channel<PulsarClientMessage>

/// What creating a consumer or reader with a versioned schema asks the client for.
let private schemaProvider (client: PulsarClient) (topic: CompleteTopicName) =
    postAndAsyncReply (mailbox client) (fun channel -> GetSchemaProvider(channel, topic))

[<Tests>]
let tests =
    testList "PulsarClient" [
        testTask "A request that fails doesn't stop the client" {
            let client = newClient ()
            // a topic without a name can't be looked up
            let failing = schemaProvider client Unchecked.defaultof<CompleteTopicName>
            let! finished = Task.WhenAny(failing, Task.Delay(TimeSpan.FromSeconds(5.0)))
            Expect.isTrue (obj.ReferenceEquals(finished, failing)) "The request must not wait for a reply that can't come"
            Expect.isTrue failing.IsFaulted "The request fails"

            // as in the Java client, the client goes on: it still answers, and it still closes
            let! _ = (schemaProvider client %"persistent://public/default/topic").WaitAsync(TimeSpan.FromSeconds(5.0))
            do! client.CloseAsync().WaitAsync(TimeSpan.FromSeconds(5.0))
        }

        testTask "A closed client takes no more requests" {
            let client = newClient ()
            do! client.CloseAsync().WaitAsync(TimeSpan.FromSeconds(5.0))

            // its mailbox is completed, so a request that still gets there is refused instead of left waiting
            do! (mailbox client).Reader.Completion.WaitAsync(TimeSpan.FromSeconds(5.0))
            Expect.throwsT<AlreadyClosedException> (fun () -> client.CloseAsync() |> ignore) "The client is closed"
        }
    ]
