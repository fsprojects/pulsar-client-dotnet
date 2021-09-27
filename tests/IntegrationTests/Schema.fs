module Pulsar.Client.IntegrationTests.Schema

open System.ComponentModel.DataAnnotations
open AvroSchemaGenerator.Attributes
open Expecto
open Expecto.Flip
open ProtoBuf
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System
open System.Collections.Generic
open Pulsar.Client.Api

[<CLIMutable>]
type SimpleRecord =
    {
        Name: string
        Age: int
    }
    
[<CLIMutable; Aliases("SimpleRecord")>]
type SimpleRecord2 =
    {
        Name: string
        Age: int
        Surname: string
    }
    
[<CLIMutable>]
type SimpleRecord3 =
    {
        Name: string
        Age: int
        [<LogicalType(LogicalTypeKind.Date)>]
        Date: DateTime
    }
    
[<CLIMutable>]
type IncompatibleRecord =
    {
        Name: int
        Age: string
    }
    
[<CLIMutable>]
[<ProtoContract>]
type SimpleProtoRecord =
    {
        [<ProtoMember(1)>]
        Name: string
        [<ProtoMember(2)>]
        Age: int
    }

[<Tests>]
let tests =

    testList "schema" [

        testAsync "String schema works fine" {

            Log.Debug("Start String schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "stringSchema"

            let! producer =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let sentText = "Hello schema"
            let! _ = producer.SendAsync(sentText) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask

            Expect.equal "" sentText (msg.GetValue())

            Log.Debug("Finished String schema works fine")
        }
        
        testAsync "Json schema works fine" {

            Log.Debug("Start Json schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "jsonSchema"

            let! producer =
                client.NewProducer(Schema.JSON<SimpleRecord3>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.JSON<SimpleRecord3>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleRecord3.Name = "abc"; Age = 20; Date = DateTime.UtcNow }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Json schema works fine")
        }
        
        testAsync "KeyValue schema separated works fine" {

            Log.Debug("Start KeyValue schema separated works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "keyValueSeparatedSchema"

            let! producer =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask
            
            Expect.isTrue "" msg.HasBase64EncodedKey
            Expect.equal "" "one" (msg.GetValue()).Value
            Expect.equal "" true (msg.GetValue()).Key

            Log.Debug("Finished KeyValue schema separated works fine")
        }
        
        testAsync "KeyValue schema inline works fine" {

            Log.Debug("Start KeyValue schema inline works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "keyValueInlineSchema"

            let! producer =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask
            
            Expect.isFalse "" msg.HasBase64EncodedKey
            Expect.equal "" "one" (msg.GetValue()).Value
            Expect.equal "" true (msg.GetValue()).Key

            Log.Debug("Finished KeyValue schema inline works fine")
        }
        
        testAsync "Protobuf schema works fine" {

            Log.Debug("Start Protobuf schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "protobufSchema"

            let! producer =
                client.NewProducer(Schema.PROTOBUF<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.PROTOBUF<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Protobuf schema works fine")
        }
        
        testAsync "ProtobufNative schema works fine" {

            Log.Debug("Start ProtobufNative schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "protobufNativeSchema"

            let! producer =
                client.NewProducer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished ProtobufNative schema works fine")
        }
        
        testAsync "Avro schema works fine" {

            Log.Debug("Start Avro schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "avroSchema"

            let! producer =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Avro schema works fine")
        }
        
        testAsync "Incompatible record errors" {

            Log.Debug("Start Incompatible record errors")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "incompatibleSchema"

            let! _ =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            Expect.throwsT2<IncompatibleSchemaException> (fun () ->                    
                    client.NewConsumer(Schema.AVRO<IncompatibleRecord>())
                        .Topic(topicName)
                        .ConsumerName(name)
                        .SubscriptionName("test-subscription")
                        .SubscribeAsync().GetAwaiter().GetResult() |> ignore
                ) |> ignore

            Log.Debug("Finished Incompatible record errors")
        }
        
        ptestAsync "Avro schema upgrade works fine" {

            Log.Debug("Start Avro schema upgrade works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName("avroUpgradeSchema1")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ConsumerName("avroUpgradeSchema1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask
            Expect.equal "" input (msg.GetValue())
            
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask            
                    
            let! producer2 =
                client.NewProducer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .ProducerName("avroUpgradeSchema2")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer2 =
                client.NewConsumer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .ConsumerName("avroUpgradeSchema2")
                    .SubscriptionName("test-subscription2")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() |> Async.AwaitTask

            let input2 = { SimpleRecord2.Name = "abc"; Age = 20; Surname = "Jones" }
            let! _ = producer2.SendAsync(input2) |> Async.AwaitTask

            let! msg1 = consumer2.ReceiveAsync() |> Async.AwaitTask
            do! consumer2.AcknowledgeAsync msg1.MessageId |> Async.AwaitTask
            let! msg2 = consumer2.ReceiveAsync() |> Async.AwaitTask
            do! consumer2.AcknowledgeAsync msg2.MessageId |> Async.AwaitTask
            do! consumer2.UnsubscribeAsync() |> Async.AwaitTask
            Expect.equal "" { input2 with Surname = null } (msg1.GetValue())
            Expect.equal "" input2 (msg2.GetValue())
            
            Log.Debug("Finished Avro schema upgrade works fine")
        }
        testAsync "Autoproduce and autoConsume protobuf native schema works fine" {
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")          

            let! producer =
                client.NewProducer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName1)
                    .ProducerName("autoNormal")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName1)
                    .ConsumerName("autoConsume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg1 = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg1.MessageId |> Async.AwaitTask
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
            let genericRecord = msg1.GetValue()
            Expect.equal "" input.Name (genericRecord.GetField("Name") |> unbox)
            Expect.equal "" input.Age (genericRecord.GetField("Age") |> unbox)
            
            
            Log.Debug("Finished Autoproduce protobuf native schema works fine")
        }

        testAsync "Autoproduce and autoConsume schema works fine" {

            Log.Debug("Start Autoproduce schema works fine")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName1)
                    .ProducerName("autoNormal")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName1)
                    .ConsumerName("autoConsume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg1 = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg1.MessageId |> Async.AwaitTask
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
            let genericRecord = msg1.GetValue()
            Expect.equal "" input.Name (genericRecord.GetField("Name") |> unbox)
            Expect.equal "" input.Age (genericRecord.GetField("Age") |> unbox)
            
            
            let! consumer2 =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName2)
                    .ConsumerName("autoNormal")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() |> Async.AwaitTask
            
            let! producer2 =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName2)
                    .ProducerName("autoProduce")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! _ = producer2.SendAsync(msg1.Data) |> Async.AwaitTask

            let! msg1 = consumer2.ReceiveAsync() |> Async.AwaitTask
            do! consumer2.UnsubscribeAsync() |> Async.AwaitTask
            let avroRecord = msg1.GetValue()
            Expect.equal "" input.Age avroRecord.Age
            Expect.equal "" input.Name avroRecord.Name
            
            Log.Debug("Finished Autoproduce schema works fine")
        }
        
        testAsync "Auto produce KeyValue with nested object works" {

            Log.Debug("Start Auto consume KeyValue with nested object works")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "autoProduceKeyValue"

            let! producer =
                client.NewProducer(Schema.KEY_VALUE<SimpleRecord, IncompatibleRecord>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! producer2 =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName)
                    .ProducerName(name + "Main")
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.KEY_VALUE<SimpleRecord, IncompatibleRecord>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let keyInput = { SimpleRecord.Name = "abc"; Age = 20 }
            let valueInput = { IncompatibleRecord.Name = 20; Age = "abc" }
            let! _ = producer.SendAsync(KeyValuePair(keyInput, valueInput)) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask
            let (KeyValue(key, value)) = msg.GetValue()
            Expect.equal "" keyInput key
            Expect.equal "" valueInput value
            
            let! _ = producer2.SendAsync(msg.Data) |> Async.AwaitTask
            let! msg2 = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg2.MessageId |> Async.AwaitTask
            let (KeyValue(key2, value2)) = msg2.GetValue()
            Expect.equal "" keyInput key2
            Expect.equal "" valueInput value2
            
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
            
          
            Log.Debug("Finished Auto consume KeyValue with nested object works")
        }
        
        ptestAsync "Auto consume with multi-version schema" {

            Log.Debug("Start Auto consume with multi-version schema")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask
                    
            let! producer2 =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName)
                    .ConsumerName("auto-consume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let simpleMessage = { SimpleRecord2.Name = "abc"; Age = 21; Surname = "test" }
            let simpleMessage2 = { SimpleRecord.Name = "def"; Age = 20 }
            let! _ = producer.SendAsync(simpleMessage) |> Async.AwaitTask // send the first version message
            let! _ = producer2.SendAsync(simpleMessage2) |> Async.AwaitTask // send the second version message

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg.MessageId |> Async.AwaitTask
            let record = msg.GetValue()
            // we could get the correct schema version for each message.
            Expect.equal "" 0L (BitConverter.ToInt64(ReadOnlySpan<byte>(record.SchemaVersion |> Array.rev)))
            Expect.equal "" simpleMessage.Name (record.GetField("Name") |> unbox)
            Expect.equal "" simpleMessage.Age (record.GetField("Age") |> unbox)
            Expect.equal "" simpleMessage.Surname (record.GetField("Surname") |> unbox)
            
            let! msg2 = consumer.ReceiveAsync() |> Async.AwaitTask
            do! consumer.AcknowledgeAsync msg2.MessageId |> Async.AwaitTask
            let record2 = msg2.GetValue()
            Expect.equal "" 1L (BitConverter.ToInt64( ReadOnlySpan<byte>(record2.SchemaVersion |> Array.rev)))
            Expect.equal "" simpleMessage2.Name (record2.GetField("Name") |> unbox)
            Expect.equal "" simpleMessage2.Age (record2.GetField("Age") |> unbox)
            
            do! consumer.UnsubscribeAsync() |> Async.AwaitTask
          
            Log.Debug("Finished Auto consume with multi-version schema")
        }
    ]
    