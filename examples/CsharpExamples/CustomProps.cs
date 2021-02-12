using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;
using System.Collections.Generic;

namespace CsharpExamples
{
    internal class CustomProps
    {
        internal static async Task RunCustomProps()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:30002";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer()
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var payload = Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'");
            var msg = producer.NewMessage(payload, "C#", new Dictionary<string, string> { ["1"] = "one" });
            var messageId = await producer.SendAsync(msg);
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data)} key: {message.Key} prop1: {message.Properties["1"]}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}