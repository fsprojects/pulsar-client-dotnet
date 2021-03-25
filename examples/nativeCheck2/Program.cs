using System;
using System.IO;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using ProtoBuf;
using Pulsar.Client.Api;

namespace nativeCheck2
{
    [DataContract]
    public class XXXMessage
    {
        [DataMember(Order = 1)]
        public string foo { get; set; }
        [DataMember(Order = 2)]
        public double bar { get; set; }
        
    }
    class Program
    {
     
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            const string serviceUrl = "pulsar://localhost:6650";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var a = Schema.JSON<XXXMessage>();
            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer(Schema.PROTOBUF_NATIVE<XXXMessage>())
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer(Schema.PROTOBUF_NATIVE<XXXMessage>())
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var messageId = await producer.SendAsync(new XXXMessage{ foo =  "sample", bar = 1.0});
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {message.GetValue()}");
            var sample =Serializer.Deserialize<XXXMessage>(message.Data.AsSpan());
            Console.WriteLine($"Received: {sample.bar} {sample.foo}");
            await consumer.AcknowledgeAsync(message.MessageId);
            Console.ReadKey();
        }
        
    }
}