using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Console;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:31002";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            Console.WriteLine("Example started");

            PulsarClient.Logger = new ConsoleLogger("PulsarLogger", (x, y) => true, true);

            var client = new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Build();

            var producer = await new ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync();

            var consumer = await new ConsumerBuilder(client)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var messageId = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId}'");


            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Payload)}");

            await consumer.AcknowledgeAsync(message.MessageId);

            Console.WriteLine("Example ended. Press any key to exit");
            Console.ReadKey();
        }
    }
}