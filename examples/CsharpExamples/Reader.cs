using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class ReaderApi
    {
        internal static async Task RunReader()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:30002";

            // retention should be set on topic so messages won't disappear
            var topicName = $"public/retention/my-topic-{DateTime.Now.Ticks}";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer()
                .Topic(topicName)
                .CreateAsync();

            var messageId1 = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 1 from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId1}'");
            var messageId2 = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 2 from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId2}'");

            var reader = await client.NewReader()
                .Topic(topicName)
                .StartMessageId(messageId1)
                .CreateAsync();

            var message = await reader.ReadNextAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data)}");
        }
    }
}