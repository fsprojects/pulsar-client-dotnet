module Pulsar.Client.IntegrationTests.Failover

open System
open System.Threading
open Expecto
open Expecto.Logging

open System.Threading.Tasks
open Pulsar.Client.Common
open Pulsar.Client.IntegrationTests.Common
open Serilog

[<Tests>]
let tests =
    testList "Failover" [ 

        testTask "Failover consumer works fine" {

            Log.Debug("Started Failover consumer works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "failoverProducer"
            let consumerName1 = "failoverConsumer1" //master
            let consumerName2 = "failoverConsumer2" //slave
            let numberOfMessages = 10

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() 

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName1)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() 
                    
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName2)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer1 numberOfMessages consumerName1
                        do! Task.Delay(100)
                        do! consumer1.DisposeAsync()
                    }:> Task)
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer2 numberOfMessages consumerName2
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) 

            Log.Debug("Finished Failover consumer works fine")
        }

        // TODO: uncomment and check in 2.8
        ptestTask "Failover consumer with PriorityLevel works fine" {

            Log.Debug("Started Failover consumer with PriorityLevel works fine")
            let client = getClient()
            let topicName = "public/default/partitioned6"
            let producerName = "priorityProducer"
            let consumerName1 = "priorityConsumer1" //master
            let consumerName2 = "priorityConsumer2" //slave
            let consumerName3 = "priorityConsumer3" //slave
            let consumerPriority1 = 0 //master
            let consumerPriority2 = 2 //slave
            let consumerPriority3 = 1 //slave
            let numberOfMessages = 10 

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .EnableBatching(false)
                    .ProducerName(producerName)
                    .CreateAsync() 

            let! consumer1 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName1)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .PriorityLevel(consumerPriority1)
                    .SubscribeAsync() 
                    
            let! consumer2 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName2)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .PriorityLevel(consumerPriority2)
                    .SubscribeAsync() 
                    
            let! consumer3 =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName3)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.Failover)
                    .PriorityLevel(consumerPriority3)
                    .SubscribeAsync() 

            do! Task.Delay(1000) 
            
            let messages1 = generateMessages numberOfMessages (producerName + "1")
            let messages2 = generateMessages numberOfMessages (producerName + "2")
            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! producePredefinedMessages producer messages1
                        do! producePredefinedMessages producer messages2
                    }:> Task)
                
            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        do! consumeAndVerifyMessages consumer1 consumerName1 messages1
                        do! Task.Delay(200)
                        do! consumer1.DisposeAsync()
                    }:> Task)
            
            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        do! consumeAndVerifyMessages consumer2 consumerName2 messages2
                        Log.Error("Wrong consumer to failover")
                        failwith "Wrong consumer to failover"
                    }:> Task)
                
            let consumer3Task =
                Task.Run(fun () ->
                    task {
                        do! consumeAndVerifyMessages consumer3 consumerName3 messages2
                    }:> Task)

            let! failoverTask = Task.WhenAny(consumer2Task, consumer3Task) 
            
            do! Task.WhenAll(producerTask, consumer1Task, failoverTask) 
            do! Task.Delay(110) // wait for acks

            Log.Debug("Finished Failover consumer with PriorityLevel works fine")
        }
    ]