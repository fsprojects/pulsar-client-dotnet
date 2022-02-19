module Pulsar.Client.IntegrationTests.Schema

open System.ComponentModel.DataAnnotations
open Avro.Generic
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

        testTask "String schema works fine" {

            Log.Debug("Start String schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "stringSchema"

            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() 

            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let sentText = "Hello schema"
            let! _ = producer.SendAsync(sentText) 

            let! (msg : Message<string>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 

            Expect.equal "" sentText (msg.GetValue())

            Log.Debug("Finished String schema works fine")
        }
        
        testTask "Json schema works fine" {

            Log.Debug("Start Json schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "jsonSchema"

            let! (producer : IProducer<SimpleRecord3>) =
                client.NewProducer(Schema.JSON<SimpleRecord3>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<SimpleRecord3>) =
                client.NewConsumer(Schema.JSON<SimpleRecord3>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleRecord3.Name = "abc"; Age = 20; Date = DateTime.UtcNow }
            let! _ = producer.SendAsync(input) 

            let! (msg : Message<SimpleRecord3>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Json schema works fine")
        }
        
        testTask "KeyValue schema separated works fine" {

            Log.Debug("Start KeyValue schema separated works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "keyValueSeparatedSchema"

            let! (producer : IProducer<KeyValuePair<bool,string>>) =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<KeyValuePair<bool,string>>) =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) 

            let! (msg : Message<KeyValuePair<bool,string>>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 
            
            Expect.isTrue "" msg.HasBase64EncodedKey
            Expect.equal "" "one" (msg.GetValue()).Value
            Expect.equal "" true (msg.GetValue()).Key

            Log.Debug("Finished KeyValue schema separated works fine")
        }
        
        testTask "KeyValue schema inline works fine" {

            Log.Debug("Start KeyValue schema inline works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "keyValueInlineSchema"

            let! (producer : IProducer<KeyValuePair<bool,string>>) =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<KeyValuePair<bool,string>>) =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) 

            let! (msg : Message<KeyValuePair<bool,string>>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 
            
            Expect.isFalse "" msg.HasBase64EncodedKey
            Expect.equal "" "one" (msg.GetValue()).Value
            Expect.equal "" true (msg.GetValue()).Key

            Log.Debug("Finished KeyValue schema inline works fine")
        }
        
        testTask "Protobuf schema works fine" {

            Log.Debug("Start Protobuf schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "protobufSchema"

            let! (producer : IProducer<SimpleProtoRecord>) =
                client.NewProducer(Schema.PROTOBUF<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() 

            let! (consumer : IConsumer<SimpleProtoRecord>) =
                client.NewConsumer(Schema.PROTOBUF<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) 

            let! (msg : Message<SimpleProtoRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Protobuf schema works fine")
        }
        
        testTask "ProtobufNative schema works fine" {

            Log.Debug("Start ProtobufNative schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "protobufNativeSchema"

            let! (producer : IProducer<SimpleProtoRecord>) =
                client.NewProducer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .CreateAsync() 

            let! (consumer : IConsumer<SimpleProtoRecord>) =
                client.NewConsumer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) 

            let! (msg : Message<SimpleProtoRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished ProtobufNative schema works fine")
        }
        
        testTask "Avro schema works fine" {

            Log.Debug("Start Avro schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "avroSchema"

            let! (producer : IProducer<SimpleRecord>) =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<SimpleRecord>) =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) 

            let! (msg : Message<SimpleRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 

            Expect.equal "" input (msg.GetValue())

            Log.Debug("Finished Avro schema works fine")
        }
        
        testTask "Incompatible record errors" {

            Log.Debug("Start Incompatible record errors")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "incompatibleSchema"

            let! _ =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 
                    
            Expect.throwsT2<IncompatibleSchemaException> (fun () ->                    
                    client.NewConsumer(Schema.AVRO<IncompatibleRecord>())
                        .Topic(topicName)
                        .ConsumerName(name)
                        .SubscriptionName("test-subscription")
                        .SubscribeAsync().GetAwaiter().GetResult() |> ignore
                ) |> ignore

            Log.Debug("Finished Incompatible record errors")
        }
        
        testTask "Avro schema upgrade works fine" {

            Log.Debug("Start Avro schema upgrade works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<SimpleRecord>) =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ProducerName("avroUpgradeSchema1")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<SimpleRecord>) =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .ConsumerName("avroUpgradeSchema1")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) 

            let! (msg : Message<SimpleRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 
            Expect.equal "" input (msg.GetValue())
            
            do! consumer.UnsubscribeAsync()             
                    
            let! (producer2 : IProducer<SimpleRecord2>) =
                client.NewProducer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .ProducerName("avroUpgradeSchema2")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer2 : IConsumer<SimpleRecord2>) =
                client.NewConsumer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .ConsumerName("avroUpgradeSchema2")
                    .SubscriptionName("test-subscription2")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() 

            let input2 = { SimpleRecord2.Name = "abc"; Age = 20; Surname = "Jones" }
            let! _ = producer2.SendAsync(input2) 

            let! (msg1 : Message<SimpleRecord2>) = consumer2.ReceiveAsync() 
            do! consumer2.AcknowledgeAsync msg1.MessageId 
            let! (msg2 : Message<SimpleRecord2>) = consumer2.ReceiveAsync() 
            do! consumer2.AcknowledgeAsync msg2.MessageId 
            do! consumer2.UnsubscribeAsync() 
            Expect.equal "" { input2 with Surname = null } (msg1.GetValue())
            Expect.equal "" input2 (msg2.GetValue())
            
            Log.Debug("Finished Avro schema upgrade works fine")
        }
        testTask "Autoproduce and autoConsume protobuf native schema works fine" {
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")          

            let! (producer : IProducer<SimpleProtoRecord>) =
                client.NewProducer(Schema.PROTOBUF_NATIVE<SimpleProtoRecord>())
                    .Topic(topicName1)
                    .ProducerName("autoNormal")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<GenericRecord>) =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName1)
                    .ConsumerName("autoConsume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleProtoRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) 

            let! (msg1 : Message<GenericRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg1.MessageId 
            do! consumer.UnsubscribeAsync() 
            let genericRecord = msg1.GetValue()
            Expect.equal "" input.Name (genericRecord.GetField("Name") |> unbox)
            Expect.equal "" input.Age (genericRecord.GetField("Age") |> unbox)
            
            
            Log.Debug("Finished Autoproduce protobuf native schema works fine")
        }

        testTask "Autoproduce and autoConsume schema works fine" {

            Log.Debug("Start Autoproduce schema works fine")
            let client = getClient()
            let topicName1 = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let topicName2 = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<SimpleRecord>) =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName1)
                    .ProducerName("autoNormal")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<GenericRecord>) =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName1)
                    .ConsumerName("autoConsume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let input = { SimpleRecord.Name = "abc"; Age = 20 }
            let! _ = producer.SendAsync(input) 

            let! (msg1 : Message<GenericRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg1.MessageId 
            do! consumer.UnsubscribeAsync() 
            let genericRecord = msg1.GetValue()
            Expect.equal "" input.Name (genericRecord.GetField("Name") |> unbox)
            Expect.equal "" input.Age (genericRecord.GetField("Age") |> unbox)
            
            
            let! (consumer2 : IConsumer<SimpleRecord>) =
                client.NewConsumer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName2)
                    .ConsumerName("autoNormal")
                    .SubscriptionName("test-subscription")
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync() 
            
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName2)
                    .ProducerName("autoProduce")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! _ = producer2.SendAsync(msg1.Data) 

            let! (msg1 : Message<SimpleRecord>) = consumer2.ReceiveAsync() 
            do! consumer2.UnsubscribeAsync() 
            let avroRecord = msg1.GetValue()
            Expect.equal "" input.Age avroRecord.Age
            Expect.equal "" input.Name avroRecord.Name
            
            Log.Debug("Finished Autoproduce schema works fine")
        }
        
        testTask "Auto produce KeyValue with nested object works" {

            Log.Debug("Start Auto consume KeyValue with nested object works")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let name = "autoProduceKeyValue"

            let! (producer : IProducer<KeyValuePair<SimpleRecord,IncompatibleRecord>>) =
                client.NewProducer(Schema.KEY_VALUE<SimpleRecord, IncompatibleRecord>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ProducerName(name)
                    .EnableBatching(false)
                    .CreateAsync() 
                    
            let! (producer2 : IProducer<byte[]>) =
                client.NewProducer(Schema.AUTO_PRODUCE())
                    .Topic(topicName)
                    .ProducerName(name + "Main")
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<KeyValuePair<SimpleRecord,IncompatibleRecord>>) =
                client.NewConsumer(Schema.KEY_VALUE<SimpleRecord, IncompatibleRecord>(SchemaType.AVRO))
                    .Topic(topicName)
                    .ConsumerName(name)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let keyInput = { SimpleRecord.Name = "abc"; Age = 20 }
            let valueInput = { IncompatibleRecord.Name = 20; Age = "abc" }
            let! _ = producer.SendAsync(KeyValuePair(keyInput, valueInput)) 

            let! (msg : Message<KeyValuePair<SimpleRecord,IncompatibleRecord>>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 
            let (KeyValue(key, value)) = msg.GetValue()
            Expect.equal "" keyInput key
            Expect.equal "" valueInput value
            
            let! _ = producer2.SendAsync(msg.Data) 
            let! (msg2 : Message<KeyValuePair<SimpleRecord,IncompatibleRecord>>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg2.MessageId 
            let (KeyValue(key2, value2)) = msg2.GetValue()
            Expect.equal "" keyInput key2
            Expect.equal "" valueInput value2
            
            do! consumer.UnsubscribeAsync() 
            
          
            Log.Debug("Finished Auto consume KeyValue with nested object works")
        }
        
        testTask "Auto consume with multi-version schema" {

            Log.Debug("Start Auto consume with multi-version schema")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! (producer : IProducer<SimpleRecord2>) =
                client.NewProducer(Schema.AVRO<SimpleRecord2>())
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() 
                    
            let! (producer2 : IProducer<SimpleRecord>) =
                client.NewProducer(Schema.AVRO<SimpleRecord>())
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! (consumer : IConsumer<GenericRecord>) =
                client.NewConsumer(Schema.AUTO_CONSUME())
                    .Topic(topicName)
                    .ConsumerName("auto-consume")
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let simpleMessage = { SimpleRecord2.Name = "abc"; Age = 21; Surname = "test" }
            let simpleMessage2 = { SimpleRecord.Name = "def"; Age = 20 }
            let! _ = producer.SendAsync(simpleMessage)  // send the first version message
            let! _ = producer2.SendAsync(simpleMessage2)  // send the second version message

            let! (msg : Message<GenericRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg.MessageId 
            let record = msg.GetValue()
            // we could get the correct schema version for each message.
            Expect.equal "" 0L (BitConverter.ToInt64(ReadOnlySpan<byte>(record.SchemaVersion |> Array.rev)))
            Expect.equal "" simpleMessage.Name (record.GetField("Name") |> unbox)
            Expect.equal "" simpleMessage.Age (record.GetField("Age") |> unbox)
            Expect.equal "" simpleMessage.Surname (record.GetField("Surname") |> unbox)
            
            let! (msg2 : Message<GenericRecord>) = consumer.ReceiveAsync() 
            do! consumer.AcknowledgeAsync msg2.MessageId 
            let record2 = msg2.GetValue()
            Expect.equal "" 1L (BitConverter.ToInt64( ReadOnlySpan<byte>(record2.SchemaVersion |> Array.rev)))
            Expect.equal "" simpleMessage2.Name (record2.GetField("Name") |> unbox)
            Expect.equal "" simpleMessage2.Age (record2.GetField("Age") |> unbox)
            
            do! consumer.UnsubscribeAsync() 
          
            Log.Debug("Finished Auto consume with multi-version schema")
        }
    ]
    