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
            const string serviceUrl = "pulsar://my-pulsar-cluster:31002";

            // retention should be set on topic so messages won't disappear
            var topicName = $"public/retention/my-topic-{DateTime.Now.Ticks}";

            var client = new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Build();

            var producer = await new ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync();

            var messageId1 = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 1 from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId1}'");
            var messageId2 = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 2 from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId2}'");

            var reader = await new ReaderBuilder(client)
                .Topic(topicName)
                .StartMessageId(messageId1)
                .CreateAsync();

            var message = await reader.ReadNextAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Payload)}");
        }
    }
}