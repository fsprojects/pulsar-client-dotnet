module Pulsar.Client.UnitTests.Common.TopicName

open System.Collections.Generic
open Expecto
open Expecto.Flip
open Pulsar.Client.Common
open FSharp.UMX

[<Tests>]
let tests =

    testList "TopicNameTests" [
        test "full TopicName succeeds" {
            let input = "persistent://tenant/namespace/topic"
            let topicName = TopicName(input)
            let fullTopicName = %topicName.CompleteTopicName

            fullTopicName |> Expect.equal "" input
        }

        test "three-parts TopicName succeeds" {
            let input = "tenant/namespace/topic"
            let topicName = TopicName(input)
            let fullTopicName = %topicName.CompleteTopicName

            fullTopicName |> Expect.equal "" ("persistent://" + input)
        }

        test "one-part TopicName succeeds" {
            let input = "topic"
            let topicName = TopicName(input)
            let fullTopicName = %topicName.CompleteTopicName

            fullTopicName |> Expect.equal "" ("persistent://public/default/" + input)
        }

        test "two-part TopicName fails" {
            let input = "namespace/topic"

            let tryBuildTopic() =
                TopicName(input) |> ignore

            Expect.throws "" tryBuildTopic
        }

        test "zero-part TopicName fails" {
            let input = ""

            let tryBuildTopic() =
                TopicName(input) |> ignore

            Expect.throws "" tryBuildTopic
        }

        test "to string returns raw topic name string" {
            let input = "persistent://tenant/namespace/topic"
            let topicName = TopicName(input)

            topicName.ToString() |> Expect.equal "" input
        }
        
        test "partitioned topic name is preserved" {
            let input = "persistent://public/default/topic-loop-2-partition-0"
            let topicName = TopicName(input)
            topicName.CompleteTopicName |> Expect.equal "" %input
        }
        
        test "topic name in hashSet is properly handled" {
            let input = "persistent://tenant/namespace/topic"
            let topicName = TopicName(input)
            let hs = HashSet<TopicName>()
            hs.Add(topicName) |> ignore
            let hs2 = HashSet(hs)
            hs2.ExceptWith(hs)
            Expect.isEmpty "" hs2
        }
    ]
