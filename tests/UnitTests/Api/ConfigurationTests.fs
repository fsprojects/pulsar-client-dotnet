namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests
open Pulsar.Client.Common

module ConfigurationTests =

    let private incorrectDefault propertyName =
        sprintf "Incorrect default value for '%s'" propertyName

    [<Tests>]
    let tests =

        testList "ConfigurationTests" [

            test "ProducerConfiguration should have correct defaults" {
                let config = ProducerConfiguration.Default

                config.Topic |> Expect.equal (incorrectDefault "Topic") Unchecked.defaultof<TopicName>
                config.ProducerName |> Expect.equal (incorrectDefault "ProducerName") String.Empty
                config.MaxPendingMessages |> Expect.equal (incorrectDefault "MaxPendingMessages") 1000
                config.BatchingEnabled |> Expect.equal (incorrectDefault "BatchingEnabled") true
                config.BatchingMaxMessages |> Expect.equal (incorrectDefault "BatchingMaxMessages") 1000
                config.BatchingMaxPublishDelay |> Expect.equal (incorrectDefault "MaxBatchingPublishDelay") (TimeSpan.FromMilliseconds(1.0))
                config.SendTimeout |> Expect.equal (incorrectDefault "SendTimeout") (TimeSpan.FromSeconds(30.0))
                config.CompressionType |> Expect.equal (incorrectDefault "CompressionType") (CompressionType.None)
                config.InitialSequenceId |> Expect.equal (incorrectDefault "InitialSequenceId") (None)
            }

        ]